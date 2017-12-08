package com.mogball.bullet.storm;

import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
public class IncrementSpout extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;

    private static final String RECORD_FIELD = "record";
    private static final Long DUMMY_ID = 42L;

    private static final int MS_TO_NS = 1000000;
    private int maxPerPeriod = 100;
    private int period = 1000 * MS_TO_NS;

    private Random random;
    private long periodCount = 0;
    private int generatedThisPeriod = 0;
    private long nextIntervalStart = 0;

    private static final String STRING = "uuid";
    private static final String LONG = "tuple_number";
    private static final String DOUBLE = "probability";

    public IncrementSpout(List<String> args) {
        if (args != null && args.size() >= 2) {
            maxPerPeriod = Integer.valueOf(args.get(0));
            period = Integer.valueOf(args.get(1)) * MS_TO_NS;

            if (maxPerPeriod < 1) {
                maxPerPeriod = 1;
            }
            if (period < 10000000) {
                period = 10000000;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RECORD_FIELD));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void activate() {
        random = new Random();
        nextIntervalStart = System.nanoTime();
        log.info("RandomSpout activated");
    }

    @Override
    public void deactivate() {
        log.info("RandomSpout deactivated");
    }

    @Override
    public void nextTuple() {
        long timeNow = System.nanoTime();
        if (timeNow <= nextIntervalStart && generatedThisPeriod < maxPerPeriod) {
            outputCollector.emit(new Values(generateRecord()), DUMMY_ID);
            generatedThisPeriod++;
        }
        if (timeNow > nextIntervalStart) {
            nextIntervalStart = timeNow + period;
            generatedThisPeriod = 0;
            periodCount++;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            log.error("Error: ", e);
        }
    }

    private BulletRecord generateRecord() {
        BulletRecord record = new BulletRecord();
        String uuid = UUID.randomUUID().toString();

        record.setString(STRING, uuid);
        record.setLong(LONG, (long) generatedThisPeriod);
        record.setDouble(DOUBLE, random.nextDouble());

        return record;
    }
}
