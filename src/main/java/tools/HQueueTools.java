package tools;

import common.HQueueParserException;
import utils.CollectionUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zhuifeng on 2017/5/28.
 */
public class HQueueTools {
    private static Map<String, Command> mapCommand = new LinkedHashMap<String, Command>() {
        {
            put(CommandConstants.COMMAND_CREATE, new CreateCommand());
            put(CommandConstants.COMMAND_PUT, new PutCommand());
            put(CommandConstants.COMMAND_SCAN, new ScanCommand());
        }
    };
    public static void main(String[] args){
        process(args);
    }
    public static void process(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }
        Command command = mapCommand.get(args[0]);
        if (command == null) {
            printUsage();
            return;
        }
        try {
            if (!command.parser(CollectionUtils.removeFirstElement(args))) {
                return;
            }
        } catch (HQueueParserException e) {
            return;
        }

        try {
            command.execute();
        } catch (Exception e) {
            System.out.println("ERROR: " + command.getHelpMessage() + " failed, " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void printUsage() {
        System.out.println("Usage: HQueueTool.sh COMMAND\n"
                + "    where COMMAND is one of:");
        Iterator<String> itr = mapCommand.keySet().iterator();
        while (itr.hasNext()) {
            String command = itr.next();
            String helpMessage = mapCommand.get(command).getHelpMessage();
            System.out.println(String.format("\t%-16s\t%8s", command, helpMessage));
        }
    }
}
