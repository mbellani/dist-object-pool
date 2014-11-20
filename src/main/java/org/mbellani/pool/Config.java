package org.mbellani.pool;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.TimeUnit;

public class Config {

    public static final Long DEFAULT_MAX_IDLE_INTRVL = 300L;
    private static final Long DEFAULT_ZOMBIE_DETECTION = 30L;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    private String name;
    private int initSize;
    private int size;
    private String zkConnectString;
    private Long metricIntrvl;
    private Long evictIntrvl;
    private Long zombieDetectionIntrvl = DEFAULT_ZOMBIE_DETECTION;
    private Long maxIdleIntrvl = DEFAULT_MAX_IDLE_INTRVL;
    private Integer numTestsPerEviction;
    private TimeUnit intrvlUnit = DEFAULT_TIME_UNIT;

    public Config(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getInitSize() {
        return initSize;
    }

    public void setInitSize(int initialCapacity) {
        this.initSize = initialCapacity;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int maxCapacity) {
        this.size = maxCapacity;
    }

    public String getZkConnectString() {
        return zkConnectString;
    }

    public void setZkConnectString(String zkConnectString) {
        this.zkConnectString = zkConnectString;
    }

    public int getNumTestsPerEviction() {
        return numTestsPerEviction;
    }

    public void setNumTestsPerEviction(int numTestsPerEviction) {
        this.numTestsPerEviction = numTestsPerEviction;
    }

    public Long getMetricIntrvl() {
        return metricIntrvl;
    }

    public void setMetricIntrvl(Long metricIntrvl) {
        this.metricIntrvl = metricIntrvl;
    }

    public Long getZombieDetectionIntrvl() {
        return zombieDetectionIntrvl;
    }

    public void setZombieDetectionIntrvl(Long zombieDetectionIntrvl) {
        this.zombieDetectionIntrvl = zombieDetectionIntrvl;
    }

    public Long getEvictIntrvl() {
        return evictIntrvl;
    }

    public void setEvictIntrvl(Long evictIntrvl) {
        this.evictIntrvl = evictIntrvl;
    }

    public Long getMaxIdleIntrvl() {
        return maxIdleIntrvl;
    }

    public void setMaxIdleIntrvl(Long maxIdleIntrvl) {
        this.maxIdleIntrvl = maxIdleIntrvl;
    }

    public TimeUnit getIntrvlUnit() {
        return intrvlUnit;
    }

    public void setIntrvlUnit(TimeUnit intrvlUnit) {
        this.intrvlUnit = intrvlUnit;
    }

    public boolean reportMetrics() {
        return metricIntrvl != null && metricIntrvl > 0;
    }

    public boolean evictionEnabled() {
        return evictIntrvl != null && evictIntrvl > 0;
    }

    public void validate() {
        checkState(getSize() > 0,
                "Please make sure you set the max capacity to a number greater than 0 to limit the pool size.");
        checkState(getInitSize() <= getSize(), "Please make sure initial capacity is not more than the max capacity.");
    }

    public static class Builder {
        private String name;
        private int initSize;
        private int size;
        private String zkConnectString;
        private int numTestsPerEviction;
        private Long metricIntrvl;
        private Long evictIntrvl;
        private Long zombieDetectionIntrvl;
        private Long maxIdleIntrvl;
        private TimeUnit intrvlUnit;

        public Builder(String name) {
            this.name = name;
        }

        public Builder initSize(int initSize) {
            this.initSize = initSize;
            return this;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public Builder zkConnectString(String zkConnectString) {
            this.zkConnectString = zkConnectString;
            return this;
        }

        public Builder metricIntrvl(Long metricIntrvl) {
            this.metricIntrvl = metricIntrvl;
            return this;
        }

        public Builder evictIntrvl(Long evictIntrvl) {
            this.evictIntrvl = evictIntrvl;
            return this;
        }

        public Builder maxIdleIntrvl(Long maxIdleIntrvl) {
            this.maxIdleIntrvl = maxIdleIntrvl;
            return this;

        }

        public Builder intrvlUnit(TimeUnit intrvlUnit) {
            this.intrvlUnit = intrvlUnit;
            return this;
        }

        public Builder numTestsPerEviction(int numTestsPerEviction) {
            this.numTestsPerEviction = numTestsPerEviction;
            return this;
        }

        public Builder zombieDetectionIntrvl(long intrvl) {
            this.zombieDetectionIntrvl = intrvl;
            return this;
        }

        public Config build() {
            Config c = new Config(name);
            c.setInitSize(initSize);
            c.setSize(size);
            c.setZkConnectString(zkConnectString);
            c.setNumTestsPerEviction(numTestsPerEviction);
            c.setMetricIntrvl(metricIntrvl);
            c.setEvictIntrvl(evictIntrvl);
            if (intrvlUnit != null) {
                c.setIntrvlUnit(intrvlUnit);
            }
            if (maxIdleIntrvl != null) {
                c.setMaxIdleIntrvl(maxIdleIntrvl);
            }
            if (zombieDetectionIntrvl != null) {
                c.setZombieDetectionIntrvl(zombieDetectionIntrvl);
            }
            return c;
        }
    }

}
