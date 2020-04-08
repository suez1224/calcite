package org.apache.calcite.sql.query.conversion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.*;
import javax.ws.rs.DefaultValue;

/** Options for indexing geo shape type. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class GeoShape {

  private final String tree;
  private final String precision;

  /**
   * @param tree prefix tree implementation.
   * @param precision precision for calculating optimal number of tree levels.
   */
  @JsonCreator
  public GeoShape(
      @JsonProperty("tree") @DefaultValue("geohash") @NotNull String tree,
      @JsonProperty("precision") @DefaultValue("50m") @NotNull String precision) {
    this.tree = tree;
    this.precision = precision;
  }

  @JsonProperty("tree")
  public String tree() {
    return tree;
  }

  @JsonProperty("precision")
  public String precision() {
    return precision;
  }
}

