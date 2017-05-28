package tools;

import coprclient.HQueue;
import coprclient.Message;
import coprclient.PartitionScanner;
import common.HQueueParserException;
import org.apache.commons.cli.*;
import utils.StringUtils;

import java.util.Arrays;

public class ScanCommand implements Command {
    private String name;
    private short partitionId;
    private long timestamp;

    @Override
    public String getHelpMessage() {
        return "put message to hqueue";
    }

    @Override
    public boolean parser(String[] args) throws HQueueParserException {
        System.out.println(Arrays.toString(args));
        Options option = new Options();
        option.addOption("name", true, "HQueue name");
        option.addOption("p", true, "hqueue partition id");
        option.addOption("t", true, "hqueue message timestamp");

        String formatStr = "-put -name -p -t";
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
        timestamp = StringUtils.parseLong(cl.getOptionValue("t"), 0L);

        if (!checkParams()) {
            formatter.printHelp(formatStr, option);
            throw new HQueueParserException("create command params error!");
        }
        return true;
    }

    @Override
    public void execute() throws Exception {
        HQueue hQueue = null;
        PartitionScanner scanner = null;
        try {
            hQueue = new HQueue(name);
            scanner = hQueue.getScanner(partitionId, timestamp);
            if(scanner.hasNext()){
                Message message = scanner.next();
                System.out.println(message);
                System.out.println("scan message success");
            }
        } finally {
            if(null != scanner){
                scanner.close();
            }
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
        if (timestamp < 0) {
            System.out.println("Error: hqueue message timestamp should not less than 0");
            return false;
        }
        return true;
    }

}
