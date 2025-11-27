package UI;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;
import javafx.stage.Stage;
import javafx.util.Callback;
import utils.Utilities;
import javafx.stage.Stage;

import java.util.Arrays;
import java.util.List;

public class UITextCell extends ListCell<String> {
	@Override
	public void updateItem(String item, boolean empty) {
		super.updateItem(item, empty);
		Rectangle rect = new Rectangle(100, 20);
		Label label = new Label();

		if (item != null) {
			label.setText(item);
			setGraphic(label);
		}
	}
}