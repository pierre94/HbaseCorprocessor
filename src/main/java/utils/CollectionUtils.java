package utils;

import java.util.ArrayList;
import java.util.List;

public class CollectionUtils {

    public static String[] removeFirstElement(String[] objects) {
        if (objects.length < 1) {
            return null;
        }
        int len = objects.length;
        String[] ret = new String[len - 1];
        System.arraycopy(objects, 0, ret, 0, len -1 );
//        for (int i = 1; i < len; i++) {
//            ret[i - 1] = objects[i];
//        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> convertObjectList(List<?> objList) {
        if (objList == null) {
            return new ArrayList<T>();
        }
        List<T> convertedList = new ArrayList<T>(objList.size());
        for (Object obj : objList) {
            convertedList.add((T) obj);
        }
        return convertedList;
    }

}
