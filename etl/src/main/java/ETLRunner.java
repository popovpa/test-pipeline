import utils.ETLPipelineBuilder;

public class ETLRunner {

    private static String TSV_IN = "data/src/data.tsv";
    private static String TSV_OUT = "data/in/correct.tsv";
    private static String INPUT = "data/in";
    private static String OUTPUT = "data/out";

    public static void main(String[] args) {
        ETLPipelineBuilder.getBuilder()
                .step(new TSVParseTask(TSV_IN, TSV_OUT))
                .step(new MRTask(INPUT, OUTPUT))
                .run();
    }
}
