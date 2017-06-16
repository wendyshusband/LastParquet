package gdut.dmir.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.io.api.Binary;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import test.Message;

import java.io.IOException;
import java.util.*;

import static parquet.schema.MessageTypeParser.parseMessageType;

/**
 * Created by Richard on 2016-07-30.
 */
public class ParquetWriter implements Writer {

    //protected Queue queue;
    //protected MessageType schema;
    protected SimpleGroupFactory factory;;
    protected ArrayList<PathAction> recorder;
    protected parquet.hadoop.ParquetWriter realWriter;

    public ParquetWriter(){
        System.setProperty("hadoop.home.dir", "H:\\123\\hadoop-common-2.2.0-bin-master");
    }

    @Override
    public boolean write(Queue queue, MessageType schema) throws IOException {
        //basic configure
        Configuration conf = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, conf);
        Path file = new Path("D:\\ideaproject\\LastParquet\\target\\tests\\TestParquetWriter.parquet");
        factory = new SimpleGroupFactory(schema);
        recorder = new ArrayList<PathAction>();

        //bulid record
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
            PathAction pathAction = new PathAction(PathAction.ActionType.FIELD);
            Type colType = schema.getType(p);
            pathAction.setType(colType.asPrimitiveType().getPrimitiveTypeName());
            pathAction.setRepetition(colType.getRepetition());
            pathAction.setName(p[p.length - 1]);
            recorder.add(pathAction);
        }

        realWriter =new parquet.hadoop.ParquetWriter<Group>(file, writeSupport,
                CompressionCodecName.GZIP,
                //ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE,
                parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE,
                parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE, // dictionary page size
                parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_2_0,
                conf
        );
        //start write
        for(int j=0;j<queue.size();j++){
            Group grp = factory.newGroup();
            Message message = (Message) queue.poll();
            String[] kv_combined = message.getValue().toString().split("\\|",-1);
            Iterator<PathAction> ai = recorder.iterator();
            //System.out.println(recorder.toString());
            Stack<Group> groupStack = new Stack<Group>();
            groupStack.push(grp);
            int i = 0;
            while(ai.hasNext()) {
                PathAction a = ai.next();
                switch (a.getAction()) {
                    case GROUPEND:
                        grp = groupStack.pop();
                       // System.out.println("GROUPEND"+a.getName());
                        break;

                    case GROUPSTART:
                        groupStack.push(grp);
                        grp = grp.addGroup(a.getName());
                        //System.out.println("GROUPSTART"+a.getName());
                        break;

                    case FIELD:
                        //System.out.println(a.getName()+a.getType()+a.getAction()+a.getRepetition());
                        String s;
                        PrimitiveType.PrimitiveTypeName primType = a.getType();
                        String colName = a.getName();
                        if (i < kv_combined.length) {
                            //System.out.println(i);
                            s = kv_combined[i ++];
                           // System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa"+i);
                        } else {
                            if (a.getRepetition() == Type.Repetition.OPTIONAL) {
                                i ++;
                                continue;
                            }
                            s = primType == PrimitiveType.PrimitiveTypeName.BINARY ? "" : "0";
                            //System.out.println(s+"optionallllllllllllllllllllllllllllllllllllll");
                        }

                        // If we have 'repeated' field, assume that we should expect JSON-encoded array
                        // Convert array and append all values
                        int repetition = 1;
                        boolean repeated = false;
                        ArrayList<String> s_vals = null;
                        //System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                        if (a.getRepetition() == Type.Repetition.REPEATED) {
                            repeated = true;
                            s_vals = new ArrayList<String>();
                            ObjectMapper mapper = new ObjectMapper();
                            //System.out.println(s+"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                            //JSONArray json = new JSONArray();
                            //json..fromObject(s);
                            JsonNode node = mapper.readTree(s);
                            Iterator <JsonNode> itr = node.getElements();
                            //for(int g=0;g<node.size();g++){
                            //    System.out.println(node.get(g).getTextValue());
                            // }
                            //System.out.println(node.size()+"Aaaaaaaaaa333333");
                            repetition = 0;
                            while(itr.hasNext()) {
                                String ass =itr.next().toString();//.getTextValue();
                               // System.out.println("aass="+ass);
                                s_vals.add(ass);  // No array-of-objects!
                                repetition ++;
                            }
                        }
                       // System.out.println("repetition="+repetition);
                        for (int t = 0; t < repetition; t ++) {

                            if (repeated)
                                // extract new s
                                s = s_vals.get(j);

                            //System.err.println(primType+s+"}}}}}}}}}}}}}}}}}}}}}}}}}");
                            try {
                                switch (primType) {

                                    case INT32:
                                       // System.out.println(s+"32aaaaaaaaaaa");
                                        grp.append(colName, Integer.parseInt(s));

                                        break;
                                    case INT64:
                                    case INT96:
                                        grp.append(colName, Long.parseLong(s));
                                      //  System.out.println(s+"96aaaaaaaa");
                                        break;
                                    case DOUBLE:
                                        grp.append(colName, Double.parseDouble(s));
                                     //   System.out.println(s+"doubleaaa");
                                        break;
                                    case FLOAT:
                                        grp.append(colName, Float.parseFloat(s));
                                      //  System.out.println(s+"floataaaaaaa");
                                        break;
                                    case BOOLEAN:
                                        grp.append(colName, s.equals("true") || s.equals("1"));
                                    //    System.out.println(s+"booleanaaaaaaaaaaaa");
                                        break;
                                    case BINARY:
                                        grp.append(colName, Binary.fromString(s));
                                      //  System.out.println(s+"binaryaaaaaaaaaaaaa");
                                        break;
                                    case FIXED_LEN_BYTE_ARRAY:
                                        grp.append(colName, Binary.fromString(s));
                                      //  System.out.println(s+"fixedaaaaaaaaaaa");
                                        break;
                                    default:
                                        throw new RuntimeException("Can't handle type " + primType);
                                }
                            } catch (NumberFormatException e) {

                                grp.append(colName, 0);
                            }
                        }
                       // System.out.println("filed"+a.getName());
                }
            }
            //System.out.println("aaxa"+grp.getFieldRepetitionCount("contacts_field"));
            //writeSupport.setSchema(schema,conf);
            //System.out.println("qwertyuiop"+writeSupport.getSchema(conf));
//new ParquetWriter(file,
            //writeSupport, CompressionCodecName.UNCOMPRESSED, blockSize, pageSize);
            realWriter.write(grp);
        }
        realWriter.close();
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
        for (int is = 0; is <3; is++) {
            Group group = reader.read();
            System.out.println(group.toString()+"~~~~~~~~~~~~~~~~~~~~~~~~~");
        }
        return true;
    }


    public static void main(String[] args) throws IOException {
        demo2 de = new demo2();

        //LinkedList<Message> queue = new LinkedList<Message>();
        LinkedList<Message> queue =demo2.s("H:\\100s.txt", 20, 275);
        System.out.println(queue.poll());
        MessageType schema = parseMessageType(
                "message test { "
                        + "required binary binary_field; "
                        + "required double int32_field; "
                        + "required double int64_field; "
                        + "required double boolean_field; "
                        + "required float float_field; "
                        + "required double double_field; "
                        + "required double contacts_field;"
                        + "required double flba_field; "
                        + "required double int96_field; "
                        + "} ");
        try {
            ParquetWriter messageToParquet = new ParquetWriter();
            //System.out.println(schema);
            messageToParquet.write(queue,schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
