public class RollingHash {
	private final int X = 31;
	private int XtoN;
	private int hash;

	public RollingHash(int blockSize) {
		XtoN = 1;
		while (blockSize-- > 0)
			XtoN *= X;
	}

	int getHash() {
		return hash;
	}

	void reset() {
		hash = 0;
	}

	void update(byte b) {
		hash = X * hash + b;
	}

	void update(byte inByte, byte outByte) {
		hash = X * hash + inByte - XtoN * outByte;
	}

	void update(byte[] arr, int offset, int len) {
		for (int i = offset; i < offset + len; i++) {
			hash = X * hash + arr[i];
		}
	}
}
