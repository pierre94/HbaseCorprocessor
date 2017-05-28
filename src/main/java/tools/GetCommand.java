package tools;

import client.HQueue;
import client.Message;
import client.MessageId;
import common.HQueueParserException;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.StringUtils;

public class GetCommand implements Command {
    private String name;
    private short partitionId;
    private long timestamp;
    private short sequenceId;

    @Override
    public String getHelpMessage() {
        return "put message to hqueue";
    }

    @Override
    public boolean parser(String[] args) throws HQueueParserException {
        Options option = new Options();
        option.addOption("name", true, "HQueue name");
        option.addOption("p", true, "hqueue partition id");
        option.addOption("t", true, "hqueue timestamp");
        option.addOption("s", true, "hqueue sequence id");

        String formatStr = "-put -name -p -t -s ";
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
        timestamp = StringUtils.parseLong(cl.getOptionValue("t"), -1L);
        sequenceId = StringUtils.parseShort(cl.getOptionValue("s"), defaultPartitionId);

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
            hQueue.get(new Message(partitionId, new MessageId(timestamp, sequenceId)));
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
