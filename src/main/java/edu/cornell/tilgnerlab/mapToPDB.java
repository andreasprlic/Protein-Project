package edu.cornell.tilgnerlab;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class mapToPDB {

    static final String userHome = System.getProperty("user.home");


    public static void main(String[] args) {


        String localDIR = userHome + "/";
        //assuming parquet file
        int cores = Runtime.getRuntime().availableProcessors();

        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("MapToPDB");



        //SparkContext sc = new SparkContext(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.stop();
        //JavaSparkContext.fromSparkContext(sc);

        SQLContext sqlContext = new SQLContext(sc);

        //register the Uniprot to PDB mapping
        DataFrame uniprotPDB = sqlContext.read().parquet(localDIR+"/dataframes.rcsb.org/parquet/uniprotpdb/20160621");

        uniprotPDB.registerTempTable("uniprotPDB"); //registerTempTable is replaced by createOrReplaceTempView

        System.out.println("PDB to uniprot mapping row:");
        uniprotPDB.show(1); //only showing the top 1 row

        //load mapping from hg38 to UniProt and look at SNP
        DataFrame chr7 = sqlContext.read().parquet(localDIR+"/dataframes.rcsb.org/parquet/humangenome/20160621/hg38/chr7");
        chr7.registerTempTable("chr7");

        DataFrame exon1 = sqlContext.sql("select * from chr11 where position = 5227002");
        System.out.println("human genome mapping to UniProt for exon1:");
        exon1.show();
    }
}