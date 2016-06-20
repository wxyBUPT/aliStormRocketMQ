package come.alibaba.middleware.race;

import com.alibaba.middleware.race.jstorm.bolt.forUpdateTair.ReportToTairThread;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/10/16.
 */
public class ReportToTairThreadTest extends TestCase{

    private static final Logger LOG = LoggerFactory.getLogger(ReportToTairThreadTest.class);

    protected ConcurrentHashMap<Long,Double> pcMiniuteTrades,mbMiniuteTrades;

    public ReportToTairThreadTest(){
        pcMiniuteTrades = new ConcurrentHashMap<Long, Double>();
        mbMiniuteTrades = new ConcurrentHashMap<Long, Double>();
    }

    public void setUp(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    Thread.sleep(1000);
                }catch (Exception e){

                }
                for(long i = 1234567890L;i<1234567901L;i++){

                    pcMiniuteTrades.put(i,(double)i+2.0);
                    mbMiniuteTrades.put(i,(double)i+2.0);
                }
                StringBuilder sb = new StringBuilder();
                LOG.info("执行了一次写操作,当前Hash Map 的值为");
                LOG.info(pcMiniuteTrades.toString());
                LOG.info(mbMiniuteTrades.toString());
            }
        }).start();
        new Thread(new ReportToTairThread(pcMiniuteTrades,mbMiniuteTrades)).start();
    }

    public void testFoo(){
        try {
            Thread.sleep(8000);
        }catch (Exception e){

        }
        assertTrue(true);
    }
}
