package tools;


import common.HQueueParserException;

public interface Command {

    String getHelpMessage();

    boolean parser(String[] args) throws HQueueParserException;

    void execute() throws Exception;
}
