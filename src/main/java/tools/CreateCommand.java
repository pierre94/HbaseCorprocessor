package tools;

import client.HQueueAdmin;
import common.HQueueParserException;
import org.apache.commons.cli.*;
import utils.StringUtils;

public class CreateCommand implements Command {
    private String name;
    private short partitionCount;

    @Override
    public String getHelpMessage() {
        return "create hqueue";
    }

    @Override
    public boolean parser(String[] args) throws HQueueParserException {
        Options option = new Options();
        option.addOption("name", true, "HQueue name");
        option.addOption("p", true, "hqueue partition num");
        option.addOption("h", false, "print help for the command");

        String formatStr = "-create -name -p ";
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(option, args);
        } catch (ParseException e) {
            formatter.printHelp(formatStr, option);
            throw new HQueueParserException("create command params error");
        }

        if (cl.hasOption("h")) {
            formatter.printHelp(formatStr, option);
            return false;
        }
        name = cl.getOptionValue("name");
        short defaultPartitions = -1;
        partitionCount = StringUtils.parseShort(cl.getOptionValue("p"), defaultPartitions);

        if (!checkParams()) {
            formatter.printHelp(formatStr, option);
            throw new HQueueParserException("create command params error!");
        }
        return true;
    }

    @Override
    public void execute() throws Exception {
        HQueueAdmin admin = null;
        try {
            admin = new HQueueAdmin();
            admin.createHQueue(name, partitionCount);
            System.out.println("create hqueue success");
        } finally {
            if(null != admin){
                admin.close();
            }
        }
    }

    private boolean checkParams() {
        if (StringUtils.isStringEmpty(name)) {
            System.out.println("Error: hqueue name not specified!");
            return false;
        }
        if (partitionCount < 2) {
            System.out.println("Error: hqueue partitions should not less than 2");
            return false;
        }
        return true;
    }

}
