import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class JSyncTest {
	static MessageDigest sha1;
	static {
		try {
			sha1 = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	JSync jSync;
	String workingDir;
	String fileName;
	File original, modified, target;

	static byte[] sha1(File f) throws FileNotFoundException, IOException {
		sha1.reset();
		FileInputStream fin = new FileInputStream(f);
		ByteBuffer bb = fin.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, f.length());
		sha1.update(bb);
		fin.close();
		return sha1.digest();
	}

	/*
	 * creates a test file of give size
	 */
	void createFile(File f, long size) throws IOException {
		f.delete(); // delete file if already exits
		Random rand = new Random();
		RandomAccessFile fout = new RandomAccessFile(f, "rw");
		ByteBuffer bb = fout.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
		byte buf[] = new byte[1024];
		int blockCount = 0; // number of blocks written
		while (size != 0) {
			if (blockCount == 0 || rand.nextInt(10) < 7) {
				// generate new random data (70% of the times)
				rand.nextBytes(buf);
			} else {
				// copy a randomly selected old block of data
				// (30% of the times)
				fout.seek(rand.nextInt(blockCount + 1) * buf.length);
				fout.read(buf);
				fout.seek(blockCount * buf.length);
			}
			bb.put(buf, 0, (int) Math.min(buf.length, size));
			size -= Math.min(buf.length, size);
			blockCount++;
		}
		fout.close();
	}

	/*
	 * returns different sizes for test file
	 */
	long[] getSizes() {
		return new long[] { 1, 1024, 1024 - 1, 1024 + 1, 2 * 1024, 2 * 1024 - 1, 2 * 1024 + 1, 32 * 1024, 32 * 1024 - 1, 32 * 1024 + 1, 376461, 3 * 1024 * 1024 + 2745 };
	}

	/*
	 * returns a list of modifiers to be applied on test file
	 */
	List<Modifier> getModifiers() {
		List<Modifier> ret = new LinkedList<>();
		ret.add(new NoModifier(workingDir));
		ret.add(new DeleteAllContent(workingDir));
		ret.add(new RandomBytesModifier(32, 4, workingDir));
		ret.add(new RandomBytesInsert(32, 128, workingDir));
		ret.add(new RandomBytesDelete(32, 128, workingDir));
		ret.add(new TruncateEnd(64, workingDir));
		ret.add(new TruncateStart(64, workingDir));
		ret.add(new TruncateEnd(10 * 1024, workingDir));
		ret.add(new TruncateStart(10 * 1024, workingDir));
		ret.add(new ReplicateWholeFile(workingDir));
		ret.add(new Modifier(workingDir) {
			@Override
			protected void modify(RandomAccessFile raf) throws IOException {
				new RandomBytesInsert(32, 128, workingDir).modify(raf);
				raf.seek(0);
				new RandomBytesDelete(32, 128, workingDir).modify(raf);
				raf.seek(0);
				new RandomBytesModifier(32, 4, workingDir).modify(raf);
				raf.seek(0);
				new TruncateEnd(64, workingDir).modify(raf);
				raf.seek(0);
				new TruncateStart(64, workingDir).modify(raf);
			}

			@Override
			String getDescription() {
				return "Composite modifier";
			}
		});
		return ret;
	}

	void f(File f) throws IOException {
		Map<String, Integer> map = new HashMap<>();
		FileInputStream fin = new FileInputStream(f);
		byte buf[] = new byte[1024];
		int count = 0;
		while (fin.available() > 0) {
			sha1.reset();
			fin.read(buf);
			sha1.digest(buf);
			byte hash[] = sha1.digest(buf);
			StringBuilder sb = new StringBuilder();
			for (byte b : hash) {
				sb.append(String.format("%02x", b));
			}
			map.put(sb.toString(), map.getOrDefault(sb.toString(), 0) + 1);
			count++;
		}
		fin.close();
		System.out.println(count);
		for (Entry<String, Integer> e : map.entrySet())
			System.out.println(e.getKey() + " -> " + e.getValue() + (e.getValue() > 1 ? " ******" : ""));
	}

	/*
	 * copy file1 into file2
	 */
	void copy(File f1, File f2) throws IOException {
		f2.delete();
		FileInputStream fin1 = new FileInputStream(f1);
		FileOutputStream fin2 = new FileOutputStream(f2);
		FileChannel fch1 = fin1.getChannel();
		FileChannel fch2 = fin2.getChannel();
		fch1.transferTo(0, fch1.size(), fch2);
		fin1.close();
		fin2.close();
	}

	void sync() throws DigestException, IOException, InvalidSignatureFile {
		File sigFile = new File(workingDir + "sig"), deltaFile = new File(workingDir + "delta");
		jSync.generateSigFile(modified, sigFile);
		jSync.generateDeltaFile(original, sigFile, deltaFile);
		jSync.applyDelta(modified, deltaFile, target);
		System.out.println("Sig len : " + sigFile.length() + " | Delta len : " + deltaFile.length());
		sigFile.delete();
		deltaFile.delete();
	}

	@Before
	public void setUp() throws Exception {
		workingDir = "/home/shivam/jsync/testing/";
		jSync = new JSync(2 * 1024);
		fileName = "f";
		original = new File(workingDir + fileName + "_original");
		modified = new File(workingDir + fileName + "_modified");
		target = new File(workingDir + fileName + "_target");
		createFile(original, 487524);
	}

	@Test
	public void testSync() throws IOException, DigestException, InvalidSignatureFile {
		for (long size : getSizes()) {
			System.out.println("*****************************************************************************");
			System.out.println("Size : " + size);

			// create test file
			createFile(original, size);

			for (Modifier modifier : getModifiers()) {
				System.out.println("-------------------------------------------------------------");
				System.out.println("Modifier : " + modifier.getDescription() + " | size : " + size);

				// make copy
				copy(original, modified);
				// modify copy
				modifier.modify(modified);

				sync();
				assertArrayEquals(sha1(original), sha1(target));
			}
		}
		original.delete();
		modified.delete();
		target.delete();
	}
}