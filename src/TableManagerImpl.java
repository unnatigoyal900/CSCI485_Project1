import java.util.HashMap;

//import ClassProjectStage1.src.AttributeType;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import com.apple.foundationdb.Range;
//import ClassProjectStage1.AttributeType;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransactionContext;

import com.apple.foundationdb.directory.DirectorySubspace;



//import javax.xml.crypto.dsig.keyinfo.KeyValue;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  private Database db;
  private DirectorySubspace rootDirectory;

  public TableManagerImpl(){
    FDB fdb = FDB.selectAPIVersion(710);
    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,PathUtil.from("Company2")).join();

    System.out.println("Open FDB Successfully!");
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType, String[] primaryKeyAttributeNames) {
    // your code

    //tableName already exists
    
    ReadTransactionContext txn = db.createTransaction();
    List<String> subdirectories = rootDirectory.list(txn).join();
    if (subdirectories.contains(tableName)) {
      return StatusCode.TABLE_ALREADY_EXISTS;
    }
    // Transaction tx = db.createTransaction();
    // //final String[] path = tableName.split(",");
    // final DirectorySubspace tableCreation = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
    // if(attributeNames == null || attributeNames.length <= 0){
    //   return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    // }
    // //attributeNames/attributeTypes is null or does not have equal length
    // for(String attName : attributeNames){
    //   if(attName == null){
    //     return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    //   }
    // }
    // if(attributeType == null || attributeType.length != attributeNames.length){
    //   return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    // }
    // for(AttributeType attype : attributeType){
    //   if(attype == null){
    //     return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    //   }
    // }
    // //no primary key attribute is specified
    // if(primaryKeyAttributeNames == null || primaryKeyAttributeNames.length <= 0){
    //   return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    // }
    // int flag = 0;
    // Tuple primary = new Tuple();
    // for(String prikey : primaryKeyAttributeNames){
    //   if(prikey .equals("")){
    //     return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    //   }
    //   else{
    //     for(String attName : attributeNames){
    //       if(attName.equals(prikey)){
    //         flag++;
    //         primary = primary.add(1);
    //         //break;
    //       }
    //       else{
    //         primary = primary.add(0);
    //       }
    //     }
    //     if(flag == 0){
    //       return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    //     }
    //   }
    // }

    // //the attribute type is not supported
    // for(AttributeType attype : attributeType){
    //   if(!(attype == AttributeType.INT || attype == AttributeType.VARCHAR || attype == AttributeType.DOUBLE)){
    //     return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
    //   }
    // }

    Transaction tx = db.createTransaction();
    Tuple primary = new Tuple();
    // byte[] existingTable = rootDirectory.get(tx, PathUtil.from(tableName)).join();
    // if (existingTable != null) {
    //     return StatusCode.TABLE_ALREADY_EXISTS;
    // }

    // Check that attributeNames, attributeType, and primaryKeyAttributeNames are valid
    if (attributeNames == null || attributeNames.length == 0
            || attributeType == null || attributeType.length != attributeNames.length
            || primaryKeyAttributeNames == null || primaryKeyAttributeNames.length == 0) {
        return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    for (String name : attributeNames) {
        if (name == null || name.isEmpty()) {
            return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
        }
    }
    for (AttributeType type : attributeType) {
        if (type == null) {
            return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
        }
    }
    for (String name : primaryKeyAttributeNames) {
        if (name == null || name.isEmpty()) {
            return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
        }
        boolean found = false;
        for (String attributeName : attributeNames) {
            if (attributeName.equals(name)) {
                found = true;
                primary = primary.add(1);
               // break;
            }
            else{
              primary = primary.add(0);
            }
        }
        if (!found) {
            return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
        }
    }

    
    //creating the table
    
    //rootDirectory.createOrOpen(tx, PathUtil.from(tableName)).join();
    final DirectorySubspace tableCreation = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
    for(int i = 0; i<attributeNames.length; i++){
      Tuple key = new Tuple();
      Tuple value = new Tuple();
      key = key.add(attributeNames[i]);
      value = value.addObject(attributeType[i].name()).addObject(primary.get(i));
      tx.set(tableCreation.pack(key), value.pack());
    }
    tx.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    return StatusCode.SUCCESS;
  }


  @Override
  public HashMap<String, TableMetadata> listTables() {
    HashMap<String, TableMetadata> result = new HashMap<>();
    Transaction tx = db.createTransaction();
    List<String> tableNames = rootDirectory.list(db).join();
    for (String tableName : tableNames) {
      List<String> path = new ArrayList<>();
      path.add(tableName);
      path.add("Company");
      DirectorySubspace metaDir = rootDirectory.open(db, path).join();
      Range r = metaDir.range();
      List<KeyValue> keyvalues = tx.getRange(r).asList().join();
      List<String> attributeNames = new ArrayList<>();
      List<AttributeType> attributeTypes = new ArrayList<>();
      List<String> primaryKeyAttributeNames = new ArrayList<>();
      for (KeyValue kv : keyvalues) {
        Tuple keyTuple = Tuple.fromBytes(kv.getKey());
        Tuple valueTuple = Tuple.fromBytes(kv.getValue());
        String attributeName = keyTuple.getString(0);
        AttributeType attributeType = AttributeType.valueOf(valueTuple.getString(0));
        boolean isPrimaryKey = valueTuple.getBoolean(1);
        if (isPrimaryKey) {
          primaryKeyAttributeNames.add(attributeName);
        }
        attributeNames.add(attributeName);
        attributeTypes.add(attributeType);
      }
      TableMetadata tableMetadata = new TableMetadata(attributeNames.toArray(new String[0]),
          attributeTypes.toArray(new AttributeType[0]), primaryKeyAttributeNames.toArray(new String[0]));
      result.put(tableName, tableMetadata);
    }

    return result;
  }
  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    return StatusCode.SUCCESS;
  }
}
