
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

//change backtype to org.apache
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private FileReader rd;
  private boolean allDone = false;


    @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */

    try {
      this.rd = new FileReader(conf.get("inputFile").toString());
    }catch (Exception e){
      e.printStackTrace();
    }

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */

    if(allDone == true){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
    }

    BufferedReader br = new BufferedReader(rd);
    String line;

      try {
          while((line = br.readLine()) != null){
              line = line.trim();
              if(line.length()>0){
                  _collector.emit(new Values(line));
              }
          }
      } catch (IOException e) {
          e.printStackTrace();
      }finally {
          allDone = true;
      }


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
      try {
          rd.close();
      } catch (IOException e) {
          e.printStackTrace();
      }

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
