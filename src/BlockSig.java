import java.util.Arrays;

public class BlockSig {
	int id;
	int hash; // weak hash
	byte md5[]; // strong hash

	public BlockSig(int id, int hash, byte md5[]) {
		this.id = id;
		this.hash = hash;
		this.md5 = md5;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BlockSig))
			return false;
		return hash == ((BlockSig) obj).hash && Arrays.equals(md5, ((BlockSig) obj).md5);
	}

	@Override
	public int hashCode() {
		return hash * 31 + Arrays.hashCode(md5);
	}
}
