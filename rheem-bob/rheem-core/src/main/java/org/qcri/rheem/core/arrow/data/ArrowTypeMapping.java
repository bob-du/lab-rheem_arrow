package org.qcri.rheem.core.arrow.data;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Date;
/**
 * @author: Du Qinghua
 * @date: 2020/5/19-10:12
 * @file: ArrowTypeMapping
 */
/**
 * 实现从arrow的数据类型到内部使用的数据类型之间的相互转换
 */
public class ArrowTypeMapping {

    /**
     * 从内部各个平台的数据类型到arrow的数据类型的映射
     * todo：需要支持更多的类型
     * @param internalType 内部数据类型
     * @return Arrow数据类型   仅仅是dataType(不是vector)
     */
    public ArrowType toArrowType(Object internalType) {
        if (internalType instanceof Boolean) {
            return ArrowType.Bool.INSTANCE;
        } else if (internalType instanceof Byte) {
            return new ArrowType.Int(8, true);
        } else if (internalType instanceof Short) {
            return new ArrowType.Int(8 * 2, true);
        } else if (internalType instanceof Integer) {
            return new ArrowType.Int(8 * 4, true);
        } else if (internalType instanceof Long) {
            return new ArrowType.Int(8 * 8, true);
        } else if(internalType instanceof Float) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (internalType instanceof Double) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (internalType instanceof String) {
            return ArrowType.Utf8.INSTANCE;
        }else if(internalType instanceof Date){
            return new ArrowType.Date(DateUnit.DAY);
        }else {
            throw new UnsupportedOperationException(internalType.getClass() + " is not support now!");
        }
    }


}
