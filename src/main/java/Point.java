
public record Point (
	  Double x,
	  Double y
) {
 
	@Override
    public String toString() {
        return " :" + this.x + ":" + this.y + " ";
    }
}
