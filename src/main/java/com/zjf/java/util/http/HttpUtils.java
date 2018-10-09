package com.zjf.java.util.http;

import com.zjf.java.util.common.Base64;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Description:httpclient工具类
 * Author: zhangjianfeng
 */
public class HttpUtils {

    /**
     * @param url  请求地址
     * @param args 请求参数
     * @return 字符串格式的服务器响应数据
     * @throws IOException
     * @Title: get
     * @Description: 向服务器发起get请求
     */
    public static String get(String url, Map<String, Object> args) throws IOException {
        // 请求服务器的地址
        StringBuilder fullHostUrl = new StringBuilder(url);

        // 如果参数不空，循环遍历参数
        if (args != null) {
            fullHostUrl.append("?");
            for (Entry<String, Object> entry : args.entrySet()) {
                String key = entry.getKey(); // 参数名称
                Object value = entry.getValue(); // 参数值

                // 如果是文件格式参数，获取文件数据流
                if (value instanceof File) {
                    value = new FileInputStream(File.class.cast(value));
                }
                // 数据流参数
                if (value instanceof InputStream) {
                    fullHostUrl.append(key).append("=");
                    InputStream is = InputStream.class.cast(value);
                    byte[] data = new byte[is.available()];
                    is.read(data);
                    fullHostUrl.append(URLEncoder.encode(Base64.encode(data), "UTF-8"));
                    // 关闭输入流
                    is.close();
                } else { // 其他类型参数
                    fullHostUrl.append(key).append("=").append(URLEncoder.encode(String.valueOf(value), "UTF-8"));
                }
                // 参数结尾加连接符
                fullHostUrl.append("&");
            }
        }

        URL host = new URL(fullHostUrl.toString());
        System.out.println(fullHostUrl.toString());
        HttpURLConnection connection = HttpURLConnection.class.cast(host.openConnection());
        // 设置为GET请求
        connection.setRequestMethod("GET");
        // 禁用缓存
        connection.setUseCaches(false);
        // 设置请求头参数
        connection.setRequestProperty("Connection", "Keep-Alive");
        connection.setRequestProperty("Charsert", "UTF-8");

        // 通过输入流来读取服务器响应
        int resultCode = connection.getResponseCode();
        StringBuilder response = new StringBuilder();
        if (resultCode == HttpURLConnection.HTTP_OK) {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            // 关闭输入流
            br.close();
        } else {
            response.append(resultCode);
        }
        return response.toString();
    }

    /**
     * @param url  请求地址
     * @param args 请求参数
     * @return 字符串格式的服务器响应数据
     * @throws IOException
     * @Title: post
     * @Description: 向服务器发起post请求
     */
    public static String post(String url, Map<String, Object> args) throws IOException {
        // 请求服务器的地址
        URL host = new URL(url);
        HttpURLConnection connection = HttpURLConnection.class.cast(host.openConnection());
        // 设置为POST请求
        connection.setRequestMethod("POST");
        // 发送POST请求必须设置如下两行
        connection.setDoOutput(true);
        connection.setDoInput(true);
        // 禁用缓存
        connection.setUseCaches(false);
        // 设置请求头参数
        connection.setRequestProperty("Connection", "Keep-Alive");
        connection.setRequestProperty("Charsert", "UTF-8");
        // 获取数据输出流
        DataOutputStream dos = new DataOutputStream(connection.getOutputStream());

        // 如果参数不空，循环遍历参数
        if (args != null) {
            for (Entry<String, Object> entry : args.entrySet()) {
                String key = entry.getKey(); // 参数名称
                Object value = entry.getValue(); // 参数值

                // 如果是文件格式参数，获取文件数据流
                if (value instanceof File) {
                    value = new FileInputStream(File.class.cast(value));
                }
                // 数据流参数
                if (value instanceof InputStream) {
                    dos.write((key + "=").getBytes());
                    InputStream is = InputStream.class.cast(value);
                    byte[] data = new byte[is.available()];
                    is.read(data);
                    dos.write(URLEncoder.encode(Base64.encode(data), "UTF-8").getBytes());
                    // 关闭输入流
                    is.close();
                } else { // 其他类型参数
                    dos.write((key + "=" + URLEncoder.encode(String.valueOf(value), "UTF-8")).getBytes());
                }
                // 参数结尾加连接符
                dos.write("&".getBytes());
            }
        }
        // 关闭输出流
        dos.flush();
        dos.close();

        // 通过输入流来读取服务器响应
        int resultCode = connection.getResponseCode();
        StringBuilder response = new StringBuilder();
        if (resultCode == HttpURLConnection.HTTP_OK) {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            // 关闭输入流
            br.close();
        } else {
            response.append(resultCode);
        }
        return response.toString();
    }


    /**
     * post发送json串
     * @param requestUrl
     * @param jsonStr
     * @return
     */
    public static String post(String requestUrl, String jsonStr) {
        StringBuffer sb = new StringBuffer();
        try {
            // 创建url资源
            URL url = new URL(requestUrl);
            // 建立http连接
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // 设置允许输出
            conn.setDoOutput(true);
            // 设置允许输入
            conn.setDoInput(true);
            // 设置不用缓存
            conn.setUseCaches(false);
            // 设置传递方式
            conn.setRequestMethod("POST");
            // 设置维持长连接
            conn.setRequestProperty("Connection", "Keep-Alive");
            // 设置文件字符集:
            conn.setRequestProperty("Charset", "UTF-8");
            // 转换为字节数组
            byte[] data = jsonStr.getBytes();
            // 设置文件长度
            conn.setRequestProperty("Content-Length", String.valueOf(data.length));
            // 设置文件类型:
            conn.setRequestProperty("contentType", "application/json");
            // 开始连接请求
            conn.connect();
            OutputStream out = new DataOutputStream(conn.getOutputStream());
            // 写入请求的字符串
            out.write(data);
            out.flush();
            out.close();

            System.out.println(conn.getResponseCode());

            // 请求返回的状态
            if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
                // 请求返回的数据
                InputStream in1 = conn.getInputStream();
                try {
                    String readLine = new String();
                    BufferedReader responseReader = new BufferedReader(new InputStreamReader(in1, "UTF-8"));
                    while ((readLine = responseReader.readLine()) != null) {
                        sb.append(readLine).append("\n");
                    }
                    responseReader.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            } else {
                System.out.println("error++");

            }

        } catch (Exception e) {

        }

        return sb.toString();

    }


}
