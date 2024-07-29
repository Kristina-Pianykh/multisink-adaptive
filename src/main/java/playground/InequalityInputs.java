import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class InequalityInputs {
  public String multiSinkQuery;
  public ArrayList<Integer> multiSinkNodes;
  public int fallbackNode;
  public int numMultiSinkNodes;
  public String partitioningInput;
  public ArrayList<String> queryInputs;
  public ArrayList<String> nonPartitioningInputs;
  public int steinerTreeSize;
  public HashMap<String, Integer> numNodesPerQueryInput;

  public void saveToFile(String filePath) throws IOException {
    // Save the object to a file
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(new File(filePath), this);
  }
}
