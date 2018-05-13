import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class JSync {
	private MessageDigest md5;
	private final int digestLen; // length of strong hash
	private final int sigLen; // length of block signature
	private final int blockSize;
	private RollingHash rollingHash;

	private final static byte MISMATCH = 0;
	private final static byte MATCH = 1;

	JSync(int blockSize) {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		this.blockSize = blockSize;
		rollingHash = new RollingHash(blockSize);
		digestLen = md5.getDigestLength();
		sigLen = digestLen + 4;
	}

	private void writeIntToByteArray(byte[] arr, int i) {
		arr[0] = (byte) (i >> 24);
		arr[1] = (byte) (i >> 16);
		arr[2] = (byte) (i >> 8);
		arr[3] = (byte) (i);
	}

	/*
	 * Copy the block (with id = blockId) from base file to target file
	 */
	private void copyBlock(RandomAccessFile base, BufferedOutputStream target, int blockId) throws IOException {
		base.seek(blockId * blockSize);
		byte tmp[] = new byte[blockSize];
		int len = base.read(tmp);
		target.write(tmp, 0, len);
	}

	/*
	 * Copy "len" number of bytes from delta file to target file
	 */
	private void copyMismatch(DataInputStream delta, BufferedOutputStream target, int len) throws IOException {
		byte buf[] = new byte[8024];
		while (len != 0) {
			int read = delta.read(buf, 0, Math.min(len, buf.length));
			target.write(buf, 0, read);
			len -= read;
		}
	}

	/*
	 * Get the signature of next block, signature contains a weak hash and a
	 * strong hash
	 */
	private byte[] getSig(ByteBuffer srcBuf) throws IOException, DigestException {
		byte sig[] = new byte[sigLen];
		byte[] buf = new byte[blockSize];
		int len = Math.min(blockSize, srcBuf.remaining());
		srcBuf.get(buf, 0, len);

		rollingHash.reset();
		rollingHash.update(buf, 0, len);
		writeIntToByteArray(sig, rollingHash.getHash());
		md5.reset();
		md5.update(buf, 0, len);
		md5.digest(sig, 4, digestLen);
		return sig;
	}

	/*
	 * Load the signatures from the signature file into memory. This function
	 * returns a hashmap with weak hash as keys and another hashmap as values.
	 * Inner hashmap is useful for quickly finding whether a block signature
	 * with a particular weak hash and strong hash exists. It has block
	 * signature as keys and a list of block ids as values.
	 */
	private Map<Integer, Map<BlockSig, List<Integer>>> loadSigFile(File sigFile) throws IOException, InvalidSignatureFile {
		Map<Integer, Map<BlockSig, List<Integer>>> sigMap = new HashMap<>();
		FileInputStream fin = new FileInputStream(sigFile);
		FileChannel inputChannel = fin.getChannel();
		ByteBuffer srcBuf = inputChannel.map(FileChannel.MapMode.READ_ONLY, 0, inputChannel.size());
		try {
			int id = 0;
			while (srcBuf.hasRemaining()) {
				if (srcBuf.remaining() < sigLen) // invalid signature file
					throw new InvalidSignatureFile("Signature file is invalid : " + sigFile.getAbsolutePath());
				int hash; // weak hash
				byte md5[] = new byte[digestLen]; // strong hash
				hash = srcBuf.getInt();
				srcBuf.get(md5);
				BlockSig sig = new BlockSig(id++, hash, md5);
				Map<BlockSig, List<Integer>> map = sigMap.getOrDefault(hash, new HashMap<>());
				List<Integer> blockIdlist = map.getOrDefault(sig, new LinkedList<>());
				blockIdlist.add(sig.id);
				map.put(sig, blockIdlist);
				sigMap.put(hash, map);
			}
		} finally {
			fin.close();
		}
		return sigMap;
	}

	/*
	 * Generate signature file from source file
	 */
	void generateSigFile(File source, File sigFile) throws IOException, DigestException {
		FileInputStream fin = new FileInputStream(source);
		FileChannel inputChannel = fin.getChannel();
		ByteBuffer srcBuf = inputChannel.map(FileChannel.MapMode.READ_ONLY, 0, inputChannel.size());

		// calculate length of signature file
		// (number of blocks * length of block signature)
		int len = (int) Math.ceil(inputChannel.size() * 1.0 / blockSize) * sigLen;

		sigFile.delete(); // delete signature file if it already exists

		RandomAccessFile outFile = new RandomAccessFile(sigFile, "rw");
		ByteBuffer out = outFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);
		try {
			while (srcBuf.hasRemaining()) {
				out.put(getSig(srcBuf));
			}
		} finally {
			fin.close();
			outFile.close();
		}
	}

	/*
	 * Generate delta file given a source file and a signature file
	 */
	void generateDeltaFile(File source, File sigFile, File deltaFile) throws IOException, InvalidSignatureFile {
		deltaFile.delete(); // delete delta file if it already exists

		RandomAccessFile raf = null;
		DataOutputStream deltaOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(deltaFile)));
		FileInputStream fin = new FileInputStream(source);
		ByteBuffer in = fin.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, source.length());
		int possible = 0, found = 0;
		try {
			// load signatures in memory
			Map<Integer, Map<BlockSig, List<Integer>>> sigMap = loadSigFile(sigFile);

			rollingHash.reset();

			// for holding current block data
			Queue<Byte> q = new ArrayDeque<>(blockSize);

			boolean insideMismatch = false; // if processing a mismatched region
			int mismatchLen = 0; // length of current mismatched region

			// lengths of mismatched regions
			List<Integer> lens = new ArrayList<>();
			// where to write lengths of mismatched regions in delta file
			List<Integer> pointers = new ArrayList<>();

			while (in.hasRemaining()) {
				byte inByte = in.get();
				if (q.size() < blockSize) {
					q.add(inByte);
					rollingHash.update(inByte);
				} else {
					byte outByte = q.poll();
					q.add(inByte);
					rollingHash.update(inByte, outByte);
					// record mismatched byte
					deltaOut.write(outByte);
					mismatchLen++;
				}
				// process current block
				if (q.size() == blockSize || !in.hasRemaining()) {
					// get block signatures with same weak hash
					Map<BlockSig, List<Integer>> map = sigMap.get(rollingHash.getHash());

					boolean flag = false; // if we find a matching block
					BlockSig blockSig = null;
					if (map != null) {
						possible++;
						// weak hash matches, now try to match strong hash
						// calculate strong hash
						md5.reset();
						for (byte b : q) {
							md5.update(b);
						}
						// prepare key
						blockSig = new BlockSig(0, rollingHash.getHash(), md5.digest());

						// get list of matching block signatures
						List<Integer> blockIdList = map.get(blockSig);
						if (blockIdList != null) {
							// we have at least one matching block
							// get the first matching block id
							blockSig.id = blockIdList.remove(0);

							// remove entries from maps if necessary
							if (blockIdList.isEmpty()) {
								map.remove(blockSig);
								if (map.size() == 0)
									sigMap.remove(rollingHash.getHash());
							}
							found++;
							q.clear();
							rollingHash.reset();
							flag = true; // signal matching block found
						}
					}
					if (!flag) {
						if (!insideMismatch) {
							deltaOut.write(MISMATCH);
							// save location to write length of this mismatched
							// region
							pointers.add(deltaOut.size());
							// write dummy value for now
							deltaOut.writeInt(0);
							insideMismatch = true;
						}
					} else {
						if (insideMismatch) {
							// save length of last mismatched region
							lens.add(mismatchLen);
							mismatchLen = 0;
							insideMismatch = false;
						}
						// record matching block id
						deltaOut.write(MATCH);
						deltaOut.writeInt(blockSig.id);
					}
				}
			}
			// if last block did not match then process the remaining data in
			// the queue
			if (insideMismatch) {
				for (byte b : q)
					deltaOut.write(b);
				mismatchLen += q.size();
				lens.add(mismatchLen);
			}

			deltaOut.close();

			// write lengths of mismatched regions at appropriate locations in
			// delta file
			raf = new RandomAccessFile(deltaFile, "rw");
			for (int i = 0; i < pointers.size(); i++) {
				raf.seek(pointers.get(i));
				raf.writeInt(lens.get(i));
			}
		} finally {
			fin.close();
			deltaOut.close();
			if (raf != null)
				raf.close();
		}
		System.out.println("Possible : " + possible + " | Found : " + found);
	}

	void applyDelta(File baseFile, File deltaFile, File targetFile) throws IOException {
		RandomAccessFile base = new RandomAccessFile(baseFile, "r");
		DataInputStream delta = new DataInputStream(new BufferedInputStream(new FileInputStream(deltaFile)));
		BufferedOutputStream target = new BufferedOutputStream(new FileOutputStream(targetFile));
		while (delta.available() > 0) {
			byte action = delta.readByte();
			if (action == MATCH) {
				// matching block found, copy it from base file
				int blockId = delta.readInt();
				copyBlock(base, target, blockId);
			} else {
				// mismatched region, copy it from delta file
				int len = delta.readInt();
				copyMismatch(delta, target, len);
			}
		}
		base.close();
		delta.close();
		target.close();
	}
}
