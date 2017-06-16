package gdut.dmir.parquet;


import parquet.schema.MessageType;
import test.Message;

import java.io.IOException;
import java.util.Queue;

/**
 * @author Jelly 
 */
public interface Writer<T> {

    /**
     * Write out.
     * @param queue message
     * @return if success, return true
     * */
    public boolean write(Queue<Message> queue, MessageType schema) throws IOException;//
}
