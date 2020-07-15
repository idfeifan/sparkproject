package com.ibeifeng;

import com.ibeifeng.sparkproject.util.DateUtils;

import java.util.Random;

public class test {
    public static void main(String[] args) {
        Boolean b = DateUtils.before("2012-01-01 10:10:10","2012-01-02 10:10:10");
        System.out.println(b);
        Random rand = new Random();
        int a =rand.nextInt(10);
        System.out.println(a);
    }
}
