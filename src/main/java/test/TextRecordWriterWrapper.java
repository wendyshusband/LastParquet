package test;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetRecordWriter;
import parquet.io.api.Binary;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

//import static parquet.hadoop.CodecFactory.BytesCompressor;

/**
 * Created by Richard on 2016-07-29.
 */
public class TextRecordWriterWrapper<K, V> implements RecordWriter<K, V> {
    public static void main(String[] args) throws IOException {
        /*MessageType schema = parseMessageType(
                "message test { "
                        + "required binary binary_field; "
                        + "repeated int32 int32_field; "
                        + "optional int64 int64_field; "
                        + "required boolean boolean_field; "
                        + "required float float_field; "
                        + "required double double_field; "
                        + "repeated group contacts_field {\n" +
                        "   required binary name;\n" +
                        "   optional binary phoneNumber;\n" +
                        " }"
                        + "required fixed_len_byte_array(3) flba_field; "
                        + "required int96 int96_field; "
                        + "} ");
        Configuration conf = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, conf);
        Path file = new Path("target/tests/TestParquetWriter/");
        ParquetFileWriter parquetFileWriter = new ParquetFileWriter(conf,schema,file);
        TextRecordWriterWrapper w = new TextRecordWriterWrapper(parquetRecordWriter,"local",conf,"tkl", new Progressable() {
            @Override
            public void progress() {
                System.out.print("Progressable~~~~~~~~~~~~~~");
            }
        });
        w.write(key,value);
        String[] strK = key.toString().split("\\|");
        String[] strV = value.toString().split("\\|",-1);
        String kv_combined[] = (String[]) ArrayUtils.addAll(strK, strV);

        System.out.println(strV.length);
        for(int i=0;i<strV.length;i++) {
            System.out.println(strV[i]);
        }
        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAA");
        for(int i=0;i<kv_combined.length;i++) {
            System.out.println(kv_combined[i]);
        }*/
    }
    protected ParquetRecordWriter<V> realWriter;
    protected MessageType schema;
    protected SimpleGroupFactory factory;
    private static final String TAB ="\t";
    protected ArrayList<PathAction> recorder;

    TextRecordWriterWrapper(ParquetRecordWriter w, String fs, Configuration conf, String name, Progressable progress)
            throws IOException {

        realWriter = w;
        schema = parquet.hadoop.example.GroupWriteSupport.getSchema(conf);
        factory = new SimpleGroupFactory(schema);

        recorder = new ArrayList<PathAction>();
        ArrayList<String[]> Paths = (ArrayList)schema.getPaths();
        Iterator<String[]> pi = Paths.listIterator();

        String[] prevPath = {};

        while (pi.hasNext()) {

            String p[] = pi.next();

            // Find longest common path between prev_path and current
            ArrayList<String> commonPath = new ArrayList<String>();
            int m = 0;
            for (int n = 0; n < prevPath.length; n++) {
                if (n < p.length && p[n].equals(prevPath[n])) {
                    commonPath.add(p[n]);
                } else
                    break;
            }

            // If current element is not inside previous group, restore to the group of common path
            for (int n = commonPath.size(); n < prevPath.length - 1; n++)
                recorder.add(new PathAction(PathAction.ActionType.GROUPEND));

            // If current element is not right after common path, create all required groups
            for (int n = commonPath.size(); n < p.length - 1; n++) {
                PathAction a = new PathAction(PathAction.ActionType.GROUPSTART);
                a.setName(p[n]);
                recorder.add(a);
            }

            prevPath = p;

            PathAction a = new PathAction(PathAction.ActionType.FIELD);

            Type colType = schema.getType(p);

            a.setType(colType.asPrimitiveType().getPrimitiveTypeName());
            a.setRepetition(colType.getRepetition());
            a.setName(p[p.length - 1]);

            recorder.add(a);
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        try {
            realWriter.close(null);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
    }

    @Override
    public void write(K key, V value) throws IOException {

        Group grp = factory.newGroup();

        String[] strK = key.toString().split(TAB,-1);
        String[] strV = value.toString().split(TAB,-1);
        String kv_combined[] = (String[]) ArrayUtils.addAll(strK, strV);

        Iterator<PathAction> ai = recorder.iterator();

        Stack<Group> groupStack = new Stack<Group>();
        groupStack.push(grp);
        int i = 0;

        try {
            while(ai.hasNext()) {

                PathAction a = ai.next();
                switch (a.getAction()) {
                    case GROUPEND:
                        grp = groupStack.pop();
                        break;

                    case GROUPSTART:
                        groupStack.push(grp);
                        grp = grp.addGroup(a.getName());
                        break;

                    case FIELD:
                        String s;
                        PrimitiveType.PrimitiveTypeName primType = a.getType();
                        String colName = a.getName();

                        if (i < kv_combined.length) {
                            s = kv_combined[i ++];
                        } else {
                            if (a.getRepetition() == Type.Repetition.OPTIONAL) {
                                i ++;
                                continue;
                            }
                            s = primType == PrimitiveType.PrimitiveTypeName.BINARY ? "" : "0";

                        }

                        // If we have 'repeated' field, assume that we should expect JSON-encoded array
                        // Convert array and append all values
                        int repetition = 1;
                        boolean repeated = false;
                        ArrayList<String> s_vals = null;
                        if (a.getRepetition() == Type.Repetition.REPEATED) {
                            repeated = true;
                            s_vals = new ArrayList();
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode node = mapper.readTree(s);
                            Iterator <JsonNode> itr = node.iterator();
                            repetition = 0;
                            while(itr.hasNext()) {
                                s_vals.add(itr.next().getTextValue());  // No array-of-objects!
                                repetition ++;
                            }
                        }

                        for (int j = 0; j < repetition; j ++) {

                            if (repeated)
                                // extract new s
                                s = s_vals.get(j);


                            try {
                                switch (primType) {

                                    case INT32:
                                        grp.append(colName, Integer.parseInt(s));
                                        break;
                                    case INT64:
                                    case INT96:
                                        grp.append(colName, Long.parseLong(s));
                                        break;
                                    case DOUBLE:
                                        grp.append(colName, Double.parseDouble(s));
                                        break;
                                    case FLOAT:
                                        grp.append(colName, Float.parseFloat(s));
                                        break;
                                    case BOOLEAN:
                                        grp.append(colName, s.equals("true") || s.equals("1"));
                                        break;
                                    case BINARY:
                                        grp.append(colName, Binary.fromString(s));
                                        break;
                                    default:
                                        throw new RuntimeException("Can't handle type " + primType);
                                }
                            } catch (NumberFormatException e) {

                                grp.append(colName, 0);
                            }
                        }
                }
            }

            realWriter.write(null, (V)grp);

        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
    }
}
