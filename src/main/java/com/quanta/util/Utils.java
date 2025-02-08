package com.quanta.util;

import java.io.File;
import java.util.Map;
import java.util.HashMap;

public class Utils {

    public static final String USER_HOME = System.getProperty("user.home");

    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }


    public static boolean isArray(Object o) {
        return o != null && o.getClass().isArray();
    }

    public static String[][] transpose(String[][] in) {
        int rows = in.length;
        int cols = in[0].length;

        String[][] ans = new String[cols][rows];

        for (int i = 0; i < cols; i++) {
            for (int j = 0; j < rows; j++) {
                ans[i][j] = in[j][i];
            }
        }

        return ans;
    }

    public static boolean deleteDir(File dir) {
        if (dir.exists()) {
            if (dir.isDirectory()) {
                String[] children = dir.list();
                for (String child : children) {
                    boolean success = deleteDir(new File(dir, child));
                    if (!success) {
                        return false;
                    }
                }
            }

            // The directory is now empty so delete it
            return dir.delete();
        }
        return false;
    }
}
