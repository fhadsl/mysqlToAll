package com.haosmart.mysqltoall.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.haosmart.mysqltoall.config.DbConfig;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author fujunhao
 */
public class ConfigUtil {


    private static final Map<String, DbConfig> map = new HashMap<>(16);


    private static final String ROOT_PATH = ConfigUtil.class.getResource("/").getPath();

    private static void readConfig() {
        String configPath = getFile("config.json", "com", "haosmart", "config");
        JSONArray jsonArray = JSONUtil.readJSONArray(FileUtil.file(configPath), StandardCharsets.UTF_8);
        map.clear();
        map.putAll(jsonArray.toList(DbConfig.class).stream().collect(Collectors.toMap(DbConfig::getId, t -> t)));
    }

    public static DbConfig getConfig(String id, boolean refresh) {
        if(refresh || ObjectUtil.isEmpty(map)){
            readConfig();
        }
        return map.get(id);
    }


    public static DbConfig getConfig(String id) {
        return getConfig(id, false);
    }

    private static String getFile(String fileName, String... path) {
        String s = CharSequenceUtil.join(File.separator, ROOT_PATH, path, fileName);
        return CharSequenceUtil.replace(s, "%20", " ", false);
    }
}
