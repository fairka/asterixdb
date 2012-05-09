package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;

public class AInt8Printer implements IPrinter {

    private static final long serialVersionUID = 1L;

    private static final String SUFFIX_STRING = "i8";
    private static byte[] _suffix;
    private static int _suffix_count;
    static {
        ByteArrayAccessibleOutputStream interm = new ByteArrayAccessibleOutputStream();
        DataOutput dout = new DataOutputStream(interm);
        try {
            dout.writeUTF(SUFFIX_STRING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        _suffix = interm.getByteArray();
        _suffix_count = interm.size();
    }

    public static final AInt8Printer INSTANCE = new AInt8Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        byte o = AInt8SerializerDeserializer.getByte(b, s + 1);
        try {
            WriteValueTools.writeInt(o, ps);
            WriteValueTools.writeUTF8StringNoQuotes(_suffix, 0, _suffix_count, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}