package utils;

import java.io.File;
import java.io.IOException;

public class ETLUtils {

    private ETLUtils() {
    }

    public static final String SUCCESS_FLAG = "_SUCCESS";

    public static boolean isSuccess(String path) {
        File successDir = new File(path);
        if (successDir.isFile()) {
            path = successDir.getParent();
        }
        File successFile = new File(path, SUCCESS_FLAG);
        return successFile.exists() && successFile.isFile();
    }

    public static void complete(String path) {
        File successDir = new File(path);
        if (successDir.exists() && successDir.isFile()) {
            path = successDir.getParent();
        }
        successDir = new File(path);
        if (successDir.exists() && successDir.isDirectory()) {
            try {
                new File(path, SUCCESS_FLAG).createNewFile();
            } catch (IOException ioex) {
                throw new RuntimeException(ioex);
            }
        }
    }

}