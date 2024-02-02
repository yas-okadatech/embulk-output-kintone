package org.embulk.output.kintone;

import com.kintone.client.model.User;
import com.kintone.client.model.record.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintoneColumnVisitor implements ColumnVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(KintoneColumnVisitor.class);
  private final PageReader pageReader;
  private Record record;
  private UpdateKey updateKey;
  private final Map<String, KintoneColumnOption> columnOptions;
  private String updateKeyName;

  public KintoneColumnVisitor(
      PageReader pageReader, Map<String, KintoneColumnOption> columnOptions) {
    this.pageReader = pageReader;
    this.columnOptions = columnOptions;
  }

  public KintoneColumnVisitor(
      PageReader pageReader, Map<String, KintoneColumnOption> columnOptions, String updateKeyName) {
    this.pageReader = pageReader;
    this.columnOptions = columnOptions;
    this.updateKeyName = updateKeyName;
  }

  public void setRecord(Record record) {
    this.record = record;
  }

  public void setUpdateKey(UpdateKey updateKey) {
    this.updateKey = updateKey;
  }

  private void setValue(String fieldCode, Object value, FieldType type, boolean isUpdateKey) {
    if (isUpdateKey && updateKey != null) {
      updateKey.setField(fieldCode).setValue(Objects.toString(value, ""));
    }
    String stringValue = Objects.toString(value, "");
    FieldValue fieldValue;
    switch (type) {
      case NUMBER:
        BigDecimal setValue = stringValue.equals("") ? null : new BigDecimal(stringValue);
        fieldValue = new NumberFieldValue(setValue);
        break;
      case MULTI_LINE_TEXT:
        fieldValue = new MultiLineTextFieldValue(stringValue);
        break;
      case DROP_DOWN:
        fieldValue = new DropDownFieldValue(stringValue);
        break;
      case LINK:
        fieldValue = new LinkFieldValue(stringValue);
        break;
      default:
        fieldValue = new SingleLineTextFieldValue(stringValue);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setJsonValue(String fieldCode, Value value, FieldType type) {
    FieldValue fieldValue;
    switch (type) {
      case USER_SELECT:
        if (!value.isArrayValue()) {
          throw new DataException("USER_SELECT should be an array of USER");
        }

        List<User> users = new ArrayList<>();
        ArrayValue values = value.asArrayValue();
        LOGGER.info("values {}, {}", values, values.size());
        for (Value v : values) {
          Map user = v.asMapValue().map();
          LOGGER.info("user {}, {}", user, user.getClass());
          LOGGER.info(
              "key: {}, {}", user.keySet().toArray()[0].getClass(), user.keySet().toArray()[0]);
          String name = String.valueOf(user.get(ValueFactory.newString("name")));
          String code = String.valueOf(user.get(ValueFactory.newString("code")));
          users.add(new User(name, code));
        }
        fieldValue = new UserSelectFieldValue(users);
        break;
      default:
        fieldValue = new SingleLineTextFieldValue(Objects.toString(value, ""));
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setTimestampValue(String fieldCode, Instant instant, ZoneId zoneId, FieldType type) {
    FieldValue fieldValue = null;
    ZonedDateTime datetime = instant.atZone(zoneId);
    switch (type) {
      case DATE:
        fieldValue = new DateFieldValue(datetime.toLocalDate());
        break;
      case DATETIME:
        fieldValue = new DateTimeFieldValue(datetime);
    }
    record.putField(fieldCode, fieldValue);
  }

  private void setCheckBoxValue(String fieldCode, Object value, String valueSeparator) {
    String str = String.valueOf(value);
    CheckBoxFieldValue checkBoxFieldValue = new CheckBoxFieldValue();

    if (str != null && !str.equals("")) {
      List<String> values = Arrays.asList(str.split(valueSeparator, 0));
      checkBoxFieldValue = new CheckBoxFieldValue(values);
    }
    record.putField(fieldCode, checkBoxFieldValue);
  }

  private FieldType getType(Column column, FieldType defaultType) {
    KintoneColumnOption option = columnOptions.get(column.getName());
    if (option == null) {
      return defaultType;
    } else {
      return FieldType.valueOf(option.getType());
    }
  }

  private String getFieldCode(Column column) {
    KintoneColumnOption option = columnOptions.get(column.getName());
    if (option == null) {
      return column.getName();
    } else {
      return option.getFieldCode();
    }
  }

  private ZoneId getZoneId(Column column) {
    KintoneColumnOption option = columnOptions.get(column.getName());
    if (option == null) {
      return ZoneId.of("UTC");
    }
    return ZoneId.of(option.getTimezone().orElse("UTC"));
  }

  private boolean isUpdateKey(Column column) {
    if (this.updateKeyName == null) {
      return false;
    }

    return this.updateKeyName.equals(column.getName());
  }

  private String getValueSeparator(Column column) {
    KintoneColumnOption option = columnOptions.get(column.getName());
    if (option == null) {
      return ",";
    }
    return option.getValueSeparator();
  }

  @Override
  public void booleanColumn(Column column) {
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.NUMBER);
    setValue(fieldCode, pageReader.getBoolean(column), type, isUpdateKey(column));
  }

  @Override
  public void longColumn(Column column) {
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.NUMBER);
    if (pageReader.isNull(column)) {
      setValue(fieldCode, null, type, isUpdateKey(column));
    } else {
      setValue(fieldCode, pageReader.getLong(column), type, isUpdateKey(column));
    }
  }

  @Override
  public void doubleColumn(Column column) {
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.NUMBER);
    setValue(fieldCode, pageReader.getDouble(column), type, isUpdateKey(column));
  }

  @Override
  public void stringColumn(Column column) {
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.MULTI_LINE_TEXT);
    Object value = pageReader.getString(column);
    if (type == FieldType.CHECK_BOX) {
      String stringValue = Objects.toString(value, "");
      setCheckBoxValue(fieldCode, value, getValueSeparator(column));
      return;
    }
    setValue(fieldCode, value, type, isUpdateKey(column));
  }

  @Override
  public void timestampColumn(Column column) {
    Timestamp value = pageReader.getTimestamp(column);
    if (value == null) {
      return;
    }

    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.DATETIME);
    ZoneId zoneId = getZoneId(column);
    if (type == FieldType.DATETIME) {
      zoneId = ZoneId.of("UTC");
    }
    setTimestampValue(fieldCode, value.getInstant(), zoneId, type);
  }

  @Override
  public void jsonColumn(Column column) {
    LOGGER.info("jsonColumn: {}", column.getName());
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.USER_SELECT);
    LOGGER.info("type: {}", type);
    switch (type) {
      case USER_SELECT:
        setJsonValue(fieldCode, pageReader.getJson(column), type);
        break;
      default:
        setValue(fieldCode, pageReader.getJson(column), type, isUpdateKey(column));
    }
  }
}
