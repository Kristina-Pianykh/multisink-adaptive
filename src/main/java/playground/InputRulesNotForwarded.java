import java.util.*;

public class InputRulesNotForwarded extends InputRules {

  public InputRulesNotForwarded(String inputEvent, boolean forwarded) {
    super(inputEvent, forwarded);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputRulesNotForwarded that = (InputRulesNotForwarded) o;
    return inputEvent.equals(that.inputEvent) && forwarded == that.forwarded;
  }

  @Override
  public int hashCode() {
    return Objects.hash(inputEvent, forwarded);
  }
}
