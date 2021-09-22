package vertx.model;

import com.jsoniter.annotation.JsonProperty;

/**
 * The model for the "world" database table.
 */
public final class World implements Comparable<World> {

  @JsonProperty(encoder = StringToIntEncoder.class)
  private final int id;
  @JsonProperty(encoder = StringToIntEncoder.class)
  private final int randomNumber;

  /**
   * Constructs a new world object with the given parameters.
   *
   * @param id the ID of the world
   * @param randomNumber the random number of the world
   */
  public World(int id, int randomNumber) {
    this.id = id;
    this.randomNumber = randomNumber;
  }

  public int getId() {
    return id;
  }

  public int getRandomNumber() {
    return randomNumber;
  }

  @Override
  public int compareTo(World o) {
    return Integer.compare(id, o.id);
  }

}