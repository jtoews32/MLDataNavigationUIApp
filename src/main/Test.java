import java.util.ArrayList;
iport java.util.List;

public class Test {
    

    public static void main(String[] args) {
        List<Integer>   list = new ArrayList<>();

        list.stream().filter( f ->  {
            return (f % 2 == 1);
        
        } ).collect(Collectors.toList());
    }
}


 


