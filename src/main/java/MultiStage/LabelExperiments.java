package MultiStage;

 

import javafx.application.Application;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.stage.Stage;
  

public class LabelExperiments extends Application  {


    @Override
    public void start(Stage primaryStage) throws Exception {
    	
    	
        primaryStage.setTitle("Hello World!");
        Button btn = new Button("'Hello World'");
        btn.setText("'Hello World'");
        
	//	Button btn = new Button(" >> ");
		///buttonNext.setOnAction(eve -> new NextState(series));
		
		/*
        btn.setOnAction(new EventHandler<ActionEvent>() {

            @Override
            public void handle(ActionEvent event) {
                System.out.println("Hello World!");
            }
        });
*/
		
		BorderPane root = new BorderPane();
        btn.setLayoutX(250);
        btn.setLayoutY(220);
        root.getChildren().add(btn);
        primaryStage.setScene(new Scene(root, 300, 250));
        primaryStage.show();
        
        /*
        primaryStage.setTitle(" ");

        FileInputStream input = new FileInputStream("resources/4.png");
        Image image = new Image(input);
        ImageView imageView = new ImageView(image);


    	Label label = new Label("", imageView);

        Scene scene = new Scene(label, 200, 100);
        primaryStage.setScene(scene);
        primaryStage.show();
        */
    }

    /* 
    public static void main(String[] args) {
        Application.launch(args);
    }
        */
    
    
}