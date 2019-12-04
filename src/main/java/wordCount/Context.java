package wordCount;



import java.util.HashMap;
import java.util.Map;

public class Context {

    private Map<Object,Object> contextMap = new HashMap<Object, Object>();

    public Map getMap(){
        return contextMap;
    }

    public Object get(Object key){
        return contextMap.get(key);
    }

    public void write(Object key,Object value){
        contextMap.put(key,value);
    }

}
