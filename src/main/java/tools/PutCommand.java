package tools;

import client.HQueue;
import client.Message;
import common.HQueueParserException;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class PutCommand implements Command {
    private String name;
    private short partitionId;
    private String value;

    @Override
    public String getHelpMessage() {
        return "put message to hqueue";
    }

    @Override
    public boolean parser(String[] args) throws HQueueParserException {
        Options option = new Options();
        option.addOption("name", true, "HQueue name");
        option.addOption("p", true, "hqueue partition id");
        option.addOption("v", true, "value to put to hqueue");

        String formatStr = "-put -name -p -v ";
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(option, args);
        } catch (ParseException e) {
            formatter.printHelp(formatStr, option);
            throw new HQueueParserException("create command params error:"+e.getMessage());
        }

        if (cl.hasOption("h")) {
            formatter.printHelp(formatStr, option);
            return false;
        }
        name = cl.getOptionValue("name");
        short defaultPartitionId = -1;
        partitionId = StringUtils.parseShort(cl.getOptionValue("p"), defaultPartitionId);
        value = cl.getOptionValue("v");

        if (!checkParams()) {
            formatter.printHelp(formatStr, option);
            throw new HQueueParserException("create command params error!");
        }
        return true;
    }

    @Override
    public void execute() throws Exception {
        HQueue hQueue = null;
        try {
            hQueue = new HQueue(name);
            final HQueue hqueue = hQueue;
            ExecutorService service = Executors.newFixedThreadPool(5);
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        List<Message> messages = new ArrayList<>();
                        for(int i = 0; i< 2; ++i){
                            messages.add(new Message(partitionId, Bytes.toBytes(value + i)));
                        }
                        hqueue.put(messages);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            System.out.println("put message to hqueue success");
        } finally {
            if(null != hQueue){
                hQueue.close();
            }
        }
    }

    private boolean checkParams() {
        if (StringUtils.isStringEmpty(name)) {
            System.out.println("Error: hqueue name not specified!");
            return false;
        }
        if (partitionId < 1) {
            System.out.println("Error: hqueue partitions should not less than 1");
            return false;
        }
        return true;
    }

}
