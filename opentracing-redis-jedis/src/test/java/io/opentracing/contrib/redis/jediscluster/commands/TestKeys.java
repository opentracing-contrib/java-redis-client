package io.opentracing.contrib.redis.jediscluster.commands;

import redis.clients.util.SafeEncoder;

public class TestKeys {
    public static final String foo = "{tag}foo";
    public static final String foo1 = "{tag}foo1";
    public static final String foo2 = "{tag}foo2";
    public static final String foo3 = "{tag}foo3";
    public static final String bar = "{tag}bar";
    public static final String bar1 = "{tag}bar1";
    public static final String bar2 = "{tag}bar2";
    public static final String bar3 = "{tag}bar3";
    public static final String bar10 = "{tag}bar10";
    public static final String foobar = "{tag}foobar";
    public static final String foostar = foo + "*";
    public static final String barstar = bar + "*";

    public static final String Key_resultAnd = "{tag}resultAnd";
    public static final String Key_resultOr = "{tag}resultOr";
    public static final String Key_resultXor = "{tag}resultXor";
    public static final String Key_resultNot = "{tag}resultNot";

    public static final byte[] bfoo = SafeEncoder.encode("{tag}bfoo");
    public static final byte[] bfoo1 = SafeEncoder.encode("{tag}bfoo1");
    public static final byte[] bfoo2 = SafeEncoder.encode("{tag}bfoo2");
    public static final byte[] bfoo3 = SafeEncoder.encode("{tag}bfoo3");

    public static final byte[] bbar = SafeEncoder.encode("{tag}bbar");
    public static final byte[] bbar1 = SafeEncoder.encode("{tag}bbar1");
    public static final byte[] bbar10 = SafeEncoder.encode("{tag}bbar10");
    public static final byte[] bbar2 = SafeEncoder.encode("{tag}bbar2");
    public static final byte[] bbar3 = SafeEncoder.encode("{tag}bbar3");
    public static final byte[] bfoobar = SafeEncoder.encode("{tag}bfoobar");
    public static final byte[] bfoostar = SafeEncoder.encode("{tag}bfoo*");
    public static final byte[] bbarstar = SafeEncoder.encode("{tag}bbar*");

    public static final String car = "{tag}car";
    public static final String car1 = "{tag}car1";
    public static final String car2 = "{tag}car2";
    public static final String car3 = "{tag}car3";
    public static final String car10 = "{tag}car10";
    public static final String carstar = "{tag}car*";
    public static final byte[] bcar = SafeEncoder.encode("{tag}bcar");
    public static final byte[] bcar1 = SafeEncoder.encode("{tag}bcar1");
    public static final byte[] bcar2 = SafeEncoder.encode("{tag}bcar2");
    public static final byte[] bcar10 = SafeEncoder.encode("{tag}bcar10");
    public static final byte[] bcarstar = SafeEncoder.encode("{tag}bcar*");

    public static final String dst = "{tag}dst";
    public static final byte[] bdst = SafeEncoder.encode("{tag}bdst");

}
