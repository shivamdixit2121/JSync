import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

abstract class Modifier {
	protected abstract void modify(RandomAccessFile raf) throws IOException;

	abstract String getDescription();

	String workingDir = "";

	public Modifier(String wd) {
		workingDir = wd;
	}

	long randomPositiveLong(Random rand) {
		return rand.nextLong() & (~0 >>> 1);
	}

	void modify(File f) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		modify(raf);
		raf.close();
	}
}

/* Perform no modification at all */
class NoModifier extends Modifier {
	public NoModifier(String wd) {
		super(wd);
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
	}

	@Override
	String getDescription() {
		return "No change";
	}
}

/* Delete all the content by setting the file size to 0 */
class DeleteAllContent extends Modifier {
	public DeleteAllContent(String wd) {
		super(wd);
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		raf.setLength(0);
	}

	@Override
	String getDescription() {
		return "Delete all content";
	}
}

/* Randomly select positions in file and set them to random values */
class RandomBytesModifier extends Modifier {
	int count = 16; // number of modifications
	int mlen = 1; // max number of bytes modified per modification

	public RandomBytesModifier(String wd) {
		super(wd);
	}

	public RandomBytesModifier(int c, int l, String wd) {
		super(wd);
		count = c;
		mlen = l;
	}

	@Override
	String getDescription() {
		return "Random bytes modifier (" + count + ", " + mlen + ")";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		long len = raf.length();
		if (len == 0)
			return;
		Random rand = new Random();
		byte tmp[] = new byte[mlen];
		for (int i = 0; i < count; i++) {
			raf.seek(randomPositiveLong(rand) % len);
			rand.nextBytes(tmp);
			raf.write(tmp, 0, rand.nextInt(mlen) + 1);
		}
	}
}

/* Randomly select positions in file and insert random data */
class RandomBytesInsert extends Modifier {
	int count = 16; // number of insertions
	int mlen = 1; // max number of bytes inserted per insertion

	public RandomBytesInsert(String wd) {
		super(wd);
	}

	public RandomBytesInsert(int c, int l, String wd) {
		super(wd);
		count = c;
		mlen = l;
	}

	@Override
	String getDescription() {
		return "Random bytes insert (" + count + ", " + mlen + ")";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		class Change {
			byte data[]; // data to be inserted
			long offset; // location of insertion

			Change(byte d[], long o) {
				data = d;
				offset = o;
			}
		}
		long len = raf.length();
		Random rand = new Random();

		// generate random changes
		List<Change> changes = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			byte tmp[] = new byte[rand.nextInt(mlen) + 1];
			rand.nextBytes(tmp);
			changes.add(new Change(tmp, len == 0 ? 0 : randomPositiveLong(rand) % len));
		}

		// sort changes by offset to avoid overwriting other changes
		Collections.sort(changes, (a, b) -> (int) (a.offset - b.offset));

		// temporary file to hold original file data
		RandomAccessFile tempFile = new RandomAccessFile(workingDir + "~", "rw");
		FileChannel fch = raf.getChannel();
		FileChannel tempch = tempFile.getChannel();
		fch.transferTo(0, len, tempch); // copy original file to temporary file
		fch.truncate(0);
		raf.seek(0);
		tempch.position(0);

		// apply changes
		long prevOffset = 0;
		for (int i = 0; i < changes.size(); i++) {
			byte[] data = changes.get(i).data;
			long offset = changes.get(i).offset;
			fch.transferFrom(tempch, raf.getFilePointer(), offset - prevOffset);
			raf.seek(raf.getFilePointer() + offset - prevOffset);
			raf.write(data);
			prevOffset = offset;
		}
		// copy remaining data
		fch.transferFrom(tempch, fch.position(), len - prevOffset);
		tempFile.close();
		new File(workingDir + "~").delete(); // delete temporary file
	}
}

/* Randomly select positions in file and delete data */
class RandomBytesDelete extends Modifier {
	int count = 16; // number of deletions
	int mlen = 1; // max number of bytes deleted per deletion

	public RandomBytesDelete(String wd) {
		super(wd);
	}

	public RandomBytesDelete(int c, int l, String wd) {
		super(wd);
		count = c;
		mlen = l;
	}

	@Override
	String getDescription() {
		return "Random bytes delete (" + count + ", " + mlen + ")";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		long len = raf.length();
		if (len == 0)
			return;
		Random rand = new Random();

		// generate random changes
		List<long[]> changes = new ArrayList<>();
		long tdel = 0; // total number of bytes to delete
		for (int i = 0; i < count; i++) {
			long offset = randomPositiveLong(rand) % len, dlen = rand.nextInt(mlen) + 1;
			changes.add(new long[] { offset, Math.min(dlen, len - offset - 1) });
			tdel += dlen;
		}
		if (len - tdel < 1) {
			// total bytes to delete >= length of file, so
			// delete all content
			raf.setLength(0);
			return;
		}

		// sort changes by offset for consistent deletion
		Collections.sort(changes, (a, b) -> (int) (a[0] - b[0]));

		// temporary file to hold original data
		RandomAccessFile tempFile = new RandomAccessFile(workingDir + "~", "rw");
		FileChannel fch = raf.getChannel();
		FileChannel tempch = tempFile.getChannel();
		fch.transferTo(0, len, tempch); // copy data to temporary file
		fch.truncate(0);
		raf.seek(0);
		tempch.position(0);

		// apply changes
		long prevOffset = 0;
		long filePointer = 0;
		for (int i = 0; i < changes.size(); i++) {
			long[] change = changes.get(i);
			fch.transferFrom(tempch, filePointer, change[0] - prevOffset);
			filePointer += (change[0] - prevOffset);
			tempch.position(tempch.position() + change[1]);
			prevOffset = change[0];
		}

		// copy remaining data
		fch.transferFrom(tempch, filePointer, len - prevOffset);
		tempFile.close();
		new File(workingDir + "~").delete(); // delete temporary file
	}
}

/* Delete data from end of the file */
class TruncateEnd extends Modifier {
	int mlen = 16; // max number of bytes deleted from the end

	public TruncateEnd(String wd) {
		super(wd);
	}

	public TruncateEnd(int l, String wd) {
		super(wd);
		mlen = l;
	}

	@Override
	String getDescription() {
		return "Truncate end (" + mlen + ")";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		long len = raf.length();
		Random rand = new Random();
		raf.setLength(Math.max(0, len - (rand.nextInt(mlen) + 1)));
	}
}

/* Delete data from the start of the file */
class TruncateStart extends Modifier {
	int mlen = 16; // max bytes deleted from the start

	public TruncateStart(String wd) {
		super(wd);
	}

	public TruncateStart(int l, String wd) {
		super(wd);
		mlen = l;
	}

	@Override
	String getDescription() {
		return "Truncate start (" + mlen + ")";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		long len = raf.length();
		Random rand = new Random();
		long newLen = len - (rand.nextInt(mlen) + 1);
		if (newLen < 1) {
			raf.setLength(0);
			return;
		}
		RandomAccessFile tempFile = new RandomAccessFile(workingDir + "~", "rw");
		FileChannel fch = raf.getChannel();
		FileChannel tempch = tempFile.getChannel();
		fch.transferTo(len - newLen, newLen, tempch);
		tempch.position(0);
		fch.transferFrom(tempch, 0, newLen);
		fch.truncate(newLen);
		tempFile.close();
		new File(workingDir + "~").delete();
	}
}

/*
 * Copying the file and pasting it at the end making a bigger file with 2 copies
 * of original file
 */
class ReplicateWholeFile extends Modifier {
	public ReplicateWholeFile(String wd) {
		super(wd);
	}

	@Override
	String getDescription() {
		return "Replicate whole file";
	}

	@Override
	protected void modify(RandomAccessFile raf) throws IOException {
		long len = raf.length();
		FileChannel fch = raf.getChannel();
		fch.transferFrom(fch, len, len);
	}
}