package wordCount;

public class WordCountMapper implements Mapper{
    @Override
    public void mapper(String s, Context context) {
//        String[] s1 = s.split(" ");
//        for (String str : s1) {
//            Object object = context.get(str);
//            if (context.getMap().containsKey(str)){
//                int i = (int)object;
//                context.write(str,i+1);
//            }else context.write(str,1);
//        }
    }
}
