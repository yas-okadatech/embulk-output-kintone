package org.embulk.output.kintone;

import com.kintone.client.model.Group;
import com.kintone.client.model.Organization;
import com.kintone.client.model.User;
import com.kintone.client.model.record.CheckBoxFieldValue;
import com.kintone.client.model.record.DateFieldValue;
import com.kintone.client.model.record.DateTimeFieldValue;
import com.kintone.client.model.record.DropDownFieldValue;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.FieldValue;
import com.kintone.client.model.record.GroupSelectFieldValue;
import com.kintone.client.model.record.LinkFieldValue;
import com.kintone.client.model.record.MultiLineTextFieldValue;
import com.kintone.client.model.record.NumberFieldValue;
import com.kintone.client.model.record.OrganizationSelectFieldValue;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.SingleLineTextFieldValue;
import com.kintone.client.model.record.UpdateKey;
import com.kintone.client.model.record.UserSelectFieldValue;
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
    if (value == null) {
      return;
    }

    FieldValue fieldValue;
    switch (type) {
      case USER_SELECT:
        {
          if (!value.isArrayValue()) {
            throw new DataException("USER_SELECT should be an array of USER");
          }

          List<User> users = new ArrayList<>();
          ArrayValue values = value.asArrayValue();
          for (Value v : values) {
            Map user = v.asMapValue().map();
            String name = String.valueOf(user.get(ValueFactory.newString("name")));
            String code = String.valueOf(user.get(ValueFactory.newString("code")));
            users.add(new User(name, code));
          }
          fieldValue = new UserSelectFieldValue(users);
        }
        break;
      case ORGANIZATION_SELECT:
        {
          if (!value.isArrayValue()) {
            throw new DataException("ORGANIZATION_SELECT should be an array of ORGANIZATION");
          }

          List<Organization> organizations = new ArrayList<>();
          ArrayValue values = value.asArrayValue();
          for (Value v : values) {
            Map organization = v.asMapValue().map();
            String name = String.valueOf(organization.get(ValueFactory.newString("name")));
            String code = String.valueOf(organization.get(ValueFactory.newString("code")));
            organizations.add(new Organization(name, code));
          }
          fieldValue = new OrganizationSelectFieldValue(organizations);
        }
        break;
      case GROUP_SELECT:
        {
          if (!value.isArrayValue()) {
            throw new DataException("GROUP_SELECT should be an array of GROUP");
          }

          List<Group> groups = new ArrayList<>();
          ArrayValue values = value.asArrayValue();
          for (Value v : values) {
            Map group = v.asMapValue().map();
            String name = String.valueOf(group.get(ValueFactory.newString("name")));
            String code = String.valueOf(group.get(ValueFactory.newString("code")));
            groups.add(new Group(name, code));
          }
          fieldValue = new GroupSelectFieldValue(groups);
        }
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
    String fieldCode = getFieldCode(column);
    FieldType type = getType(column, FieldType.MULTI_LINE_TEXT);
    switch (type) {
      case USER_SELECT:
      case ORGANIZATION_SELECT:
      case GROUP_SELECT:
        setJsonValue(fieldCode, pageReader.getJson(column), type);
        break;
      default:
        setValue(fieldCode, pageReader.getJson(column), type, isUpdateKey(column));
    }
  }
}
