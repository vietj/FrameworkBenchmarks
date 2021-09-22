package vertx.model;

import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;

import java.io.IOException;

public class StringToIntEncoder extends Encoder.IntEncoder {

    @Override
    public void encodeInt(int obj, JsonStream stream) throws IOException {
        stream.write('"');
        stream.writeVal(obj);
        stream.write('"');
    }
}
