
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import javax.xml.validation.Schema;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.chart.PieChart;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import scala.collection.JavaConverters;
import utils.Utilities;

import java.nio.file.Files;
import java.util.stream.IntStream;

import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.HBox;

import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import scala.collection.Seq;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaFXClass extends Application {

    public static Dataset<Row> incidentData = null;
    public static SparkSession spark = null;

    static {

        final SparkConf sparkConf = new SparkConf()
                .setAppName("CrimeAvoidanceAI")
                .setMaster("local[*]") // Run locally with all available cores
                .set("spark.executor.memory", "2g") // Set executor memory
                .set("spark.driver.memory", "2g"); // Set driver memory

        spark = SparkSession.builder().appName("").config(sparkConf).getOrCreate();

    }

    @Override
    public void start(Stage subStage) {

        PieChart pieChart = new PieChart();

        PieChart.Data slice1 = new PieChart.Data("Desktop", 213);
        PieChart.Data slice2 = new PieChart.Data("Phone", 67);
        PieChart.Data slice3 = new PieChart.Data("Tablet", 36);

        pieChart.getData().add(slice1);
        pieChart.getData().add(slice2);
        pieChart.getData().add(slice3);

        ListView listView = new ListView();

        // listView.getItems().add("File2.csv");
        // listView.getItems().add("File3.csv");
        Button button1 = new Button("Select File");
        Button button2 = new Button("Street Window");

        FileChooser fileChooser = new FileChooser();
        fileChooser.setInitialDirectory(new File("C:\\Users\\jonrt\\OneDrive\\Desktop\\Projects\\MLDataNavigationUIApp\\data"));
        //fileChooser.setInitialFileName("myfile.txt");

        /*  fileChooser.getExtensionFilters().addAll(
            new FileChooser.ExtensionFilter("Text Files", "*.txt"),
            new FileChooser.ExtensionFilter("Data Files", "*.data")
        );*/
        TableView tableView = new TableView();

        button1.setOnAction(e -> {

            File selectedFile = fileChooser.showOpenDialog(subStage);
            System.out.println("Selected file: " + selectedFile.getAbsolutePath());

            listView.getItems().add(selectedFile.getName());

            incidentData = spark
                    .read()
                    .option("header", "true")
                    .csv(selectedFile.getAbsolutePath());

            incidentData.show(5, false);
            // incidentData.printSchema();

            // Get the schema of the Dataset
            StructType schema = incidentData.schema();

            // Get the column names (header) from the schema
            String[] columnNames = schema.fieldNames();

            // Print the column names
            System.out.println("Column Names (Header):");
            for (String colName : columnNames) {

                System.out.println(colName.toString());
                TableColumn<Map, String> column1 = new TableColumn<>(colName.toString());
                column1.setCellValueFactory(new MapValueFactory<>(colName.toString().replaceAll(" ", "")));
                tableView.getColumns().add(column1);

            }



			List<Row> rows = incidentData.collectAsList();
			for (Row row : rows) {

				System.out.println("Row: GOT IT " );


				// Access individual column values within the row
			//	String columnName = row.getString(row.fieldIndex("column_name"));
			//	System.out.println("Column Name: " + columnName);

				

                Seq<Object> rowSeq = row.toSeq();

                java.util.List<Object> rowList = JavaConverters.seqAsJavaList(rowSeq);

                Map<String, Object> item1 = new HashMap<>();

                IntStream.range(0, rowList.size())
                        .forEach(i -> {
							
                            String name = columnNames[i].replaceAll(" ", "");

                            item1.put(name, rowList.get(i).toString());

                        }
                        );

                tableView.getItems().add(item1);

			}


        

            // Row row = incidentData.first();
            /* 
			incidentData.collectAsList().forEach( r -> {
				Person person = new Person( r.getString(0), r.getString(1) );
				tableView.getItems().add( person );
			} );*/
        });

        //Label nameLabel = new Label("Enter your name:");
        TextField nameField = new TextField();
        Label feedbackLabel = new Label();

     	 button2.setOnAction(e -> {
			// Run street display here. 

			/* 
            String name = nameField.getText();
            if (!name.isEmpty()) {
                feedbackLabel.setText("Hello, " + name + "!");
            } else {
                feedbackLabel.setText("Please enter a name.");
            }*/
        }); 



        VBox vbox1 = new VBox(button1, button2 );
        vbox1.setPadding(new Insets(10, 20, 10, 10));
        //vbox1.setMargin(label, new Insets(10, 10, 10, 10));

        // vbox1.setWidth(200);
        // vbox1.setHeight(300);
        HBox hbox2 = new HBox(vbox1, listView);

        VBox vbox = new VBox(hbox2, tableView);

        Scene scene = new Scene(vbox, 800, 200);


        /*  
        HBox hbox = new HBox();
        hbox.setSpacing(10);
        hbox.setPadding(new Insets(10,20, 10,10));
        hbox.setMargin(label, new Insets(10, 10, 10, 10));
        
        ObservableList hboxList = hbox.getChildren();
        hboxList.addAll(label);
         */
        // Scene scene = new Scene(hbox, 100, 100);
        subStage.setTitle("Financial Prediction Star");
        subStage.setScene(scene);

        subStage.setWidth(800);
        subStage.setHeight(300);

        subStage.show();

        /* 
        //Stage subStage = new Stage();
		//FlowPane root = new FlowPane();
		//Scene scene = new Scene(root, 300, 200);




        // Create UI controls
        Label nameLabel = new Label("Enter your name:");
        TextField nameField = new TextField();
        Button submitButton = new Button("Submit");
        Label feedbackLabel = new Label();

        // Handle button click event
        submitButton.setOnAction(e -> {
            String name = nameField.getText();
            if (!name.isEmpty()) {
                feedbackLabel.setText("Hello, " + name + "!");
            } else {
                feedbackLabel.setText("Please enter a name.");
            }
        });

        // Create a layout container (VBox for vertical arrangement)
        //VBox 
        // FlowPane root = new VBox(10); // Spacing of 10 pixels between elements
        VBox root = new VBox(10);

        ListView<String> list = new ListView<String>();
		ObservableList<String> data = FXCollections.observableArrayList();

		Label label = new Label();


        
        root.setPadding(new Insets(20)); // Padding around the container

        // Add controls to the layout container
        root.getChildren().addAll(nameLabel, nameField, submitButton, feedbackLabel);

        // Create a Scene and set it on the Stage
        Scene scene = new Scene(root, 300, 200); // Width, Height
        subStage.setTitle("Simple JavaFX UI");
        subStage.setScene(scene);
        subStage.show();
         */
    }

    public static void main(String[] args) {


        /* 


       Schema schema = new Schema.Parser().parse(
            ParquetWriteExample.class.getResourceAsStream("/user.avsc"));

        List<GenericData.Record> records = Arrays.asList(
            new GenericData.Record(schema) {{ put("name", "Alice"); put("favorite_number", 7); put("favorite_color", "blue"); }},
            new GenericData.Record(schema) {{ put("name", "Bob"); put("favorite_number", 13); put("favorite_color", null); }},
            new GenericData.Record(schema) {{ put("name", "Charlie"); put("favorite_number", null); put("favorite_color", "green"); }}
        );

        Path fileToWrite = new Path("users.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericData.Record record : records) {
                writer.write(record);
            }
            System.out.println("Parquet file written successfully to " + fileToWrite);
        }

         */
        try {
            Class.forName("org.apache.spark.sql.SparkSession");

            /* 
		
		Dataset<Row> incidentData = null;
		StructType schemaNB;
		//PipelineModel modelNB;
		//Map<String, Double> category;
		//Map<Double, String> categoryInversed;
		// JavaSparkContext javaSparkContext;
		//JavaRDD<Row> incidentDataRowRDD;
	 
		Dataset<Row> incidentFilteredTrainData;
		//PipelineModel modelRF;

      
		final SparkConf sparkConf = new SparkConf()
					.setAppName("CrimeAvoidanceAI")
					.setMaster("local[*]") // Run locally with all available cores
					.set("spark.executor.memory", "2g") // Set executor memory
					.set("spark.driver.memory", "2g"); // Set driver memory

   
        final SparkSession spark = SparkSession.builder().appName("").config(sparkConf).getOrCreate();
  
		incidentData = spark
				.read()
				.csv(utils.Utilities.getPath() + "Police_Department_Incidents.data");


	 	incidentData.show(5, false);
		incidentData.printSchema();

 

		RDD<Row> rows = incidentData.javaRDD().filter(row -> row.length()== 13).map((row) -> {
			return RowFactory.create(
					row.get(0), 
					row.get(1), 
					row.get(2), 
					utils.Utilities.DayOfWeek(row.get(3)),
					utils.Utilities.RoundDate(row.get(4)), 
					utils.Utilities.RoundTime(row.get(5)), 
					row.get(6), 
					row.get(7), 
					utils.Utilities.CleanStreetName(row.get(8).toString()),
					BigDecimal.valueOf(Double.valueOf(row.get(9).toString())).setScale(6, RoundingMode.HALF_UP).doubleValue(),
					BigDecimal.valueOf(Double.valueOf(row.get(10).toString())).setScale(6, RoundingMode.HALF_UP).doubleValue(),
					row.get(11), 
					row.get(12)
			); 
		}).rdd();    


		List<String> tableColumnsRF = Arrays.asList(
				"IncidntNum","Category","Descript","DayOfWeek","Date","Time","PdDistrict",
				"Resolution","Address","X","Y","Location","PdId"  );
		
		StructType schemaRF = Utilities.createSchema(tableColumnsRF);		

 		incidentFilteredTrainData = spark.createDataFrame(rows , schemaRF);
 

	
 

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{ "Date", "Time", "X", "Y"}).setOutputCol("features");
		StringIndexer indexer = new StringIndexer().setInputCol("Category").setOutputCol("labelIndexed");
 
		
		
		RandomForestClassifier randomForestClassifier = new RandomForestClassifier().setLabelCol("labelIndexed").setFeaturesCol("features");
		
	
		Pipeline pipelineRF = new Pipeline().setStages(new PipelineStage[] {assembler, indexer, randomForestClassifier});
		
		System.out.println("Preparing modelRF...");
		
		PipelineModel modelRF = pipelineRF.fit(incidentFilteredTrainData);

		List<String> tableColumns = Arrays.asList("id","topic","text","label");
		schemaNB = utils.Utilities.createSchemaNB(tableColumns);		

		List<Row> inputTextRow = new ArrayList<Row>();
	

 
		//try {
			// NON CRIME
 
		String content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d1.txt"))));
		inputTextRow.add( RowFactory.create("00001", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d2.txt"))));
		
		inputTextRow.add( RowFactory.create("00002", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d3.txt"))));
		
		inputTextRow.add( RowFactory.create("00003", "TRAVEL", content ,0) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "good/d4.txt"))));
		inputTextRow.add( RowFactory.create("00004", "TRAVEL", content ,0) );
		

		// CRIME
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d1.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d2.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d3.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d4.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		content = utils.Utilities.cleanText(new String(Files.readAllBytes(Paths.get(utils.Utilities.getPath() + "bad/d5.txt"))));
		inputTextRow.add( RowFactory.create("10003", "CRIME", content ,1) );
		//} catch (Exception e) {
			
		//}


		Dataset<Row> trainingData = spark.createDataFrame(inputTextRow, schemaNB);

		RegexTokenizer tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("wordsoutput");
		
		StopWordsRemover remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol()).setOutputCol("words");
		
		HashingTF hashingTF = new HashingTF()
				  .setInputCol(remover.getOutputCol()) 
				  .setOutputCol("features")
				  .setNumFeatures(5000);		  

		NaiveBayes naiveBayes = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial"); 

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, remover, hashingTF, naiveBayes});

		System.out.println("Preparing modelNB...");
		
		PipelineModel modelNB = pipeline.fit(trainingData);
		
		System.out.println("Done modelNB...");






             */
        } catch (Exception e) {
            System.out.println("Spark not found in classpath");

        }

        launch(args);
    }
}
