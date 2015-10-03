package util;

public class Record {

    final char[] read;

    public Record(final char[] data) {
        read = data;
    }

    public char[] getRead(){
        return this.read;
    }

}