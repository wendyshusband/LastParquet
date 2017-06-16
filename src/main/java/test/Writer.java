package test;


import parquet.schema.MessageType;

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
    public boolean write(MessageType schema) throws IOException;//Queue<Message> queue,
}
