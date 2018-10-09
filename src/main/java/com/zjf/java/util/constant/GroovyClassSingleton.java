package com.zjf.java.util.constant;

import com.zjf.java.util.inter.ICodingTransform;
import groovy.lang.GroovyClassLoader;

/**
 * Description:
 * Author: zhangjianfeng
 */
public class GroovyClassSingleton {

    private static GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
    private volatile static Class<ICodingTransform> instance; //声明成 volatile

    private GroovyClassSingleton() {
    }

    public static Class<ICodingTransform> getSingleton(String groovyCode) {
        if (instance == null) {
            synchronized (GroovyClassSingleton.class) {
                if (instance == null) {
                    instance = groovyClassLoader.parseClass(groovyCode);
                }
            }
        }
        return instance;
    }

}
