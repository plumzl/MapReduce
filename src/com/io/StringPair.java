package com.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable {
  public String left;
  public String right;

  public StringPair(String left, String right) {
    this.left = left;
    this.right = right;
  }

  public StringPair() {}

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, left);
    Text.writeString(out, right);
  }

  public void readFields(DataInput in) throws IOException {
    left = Text.readString(in);
    right = Text.readString(in);
  }

  public int hashCode() {
    return left.hashCode() + right.hashCode();
  }

  public int compareTo(Object obj) {
    StringPair pair = (StringPair) obj;
    if (left.equals(pair.left)) {
      // Reducer sort order so (word, *) always comes first
      if (right.equals("*") && pair.right.equals("*")) {
        return 0;
      }
      else if (right.equals("*")) {
        return -1;
      }
      else if (pair.right.equals("*")) {
        return 1;
      }
      else {
        return right.compareTo(pair.right);
      }
    }
    return left.compareTo(pair.left);
  }

  public boolean equals(Object obj) {
    if (obj instanceof StringPair) {
      StringPair pair = (StringPair) obj;
      return left.equals(pair.left) && right.equals(pair.right);
    }
    else {
      return false;
    }
  }

  public void set(String left, String right) {
    this.left = left;
    this.right = right;
  }

  public String toString() {
    return "(" + left + ", " + right + ")";
  }
}
