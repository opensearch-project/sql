package org.opensearch.sql.spark.ppl;

import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * Transform the PPL Logical comparator into catalyst comparator
 */
public class ComparatorTransformer {
    /**
     * expression builder
     * @return
     */
    public static BinaryComparison comparator(Compare expression) {
        if(BuiltinFunctionName.of(expression.getOperator()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + BuiltinFunctionName.of(expression.getOperator()));
            
        switch (BuiltinFunctionName.of(expression.getOperator()).get()) {
            case ABS:
                break;
            case CEIL:
                break;
            case CEILING:
                break;
            case CONV:
                break;
            case CRC32:
                break;
            case E:
                break;
            case EXP:
                break;
            case EXPM1:
                break;
            case FLOOR:
                break;
            case LN:
                break;
            case LOG:
                break;
            case LOG10:
                break;
            case LOG2:
                break;
            case PI:
                break;
            case POW:
                break;
            case POWER:
                break;
            case RAND:
                break;
            case RINT:
                break;
            case ROUND:
                break;
            case SIGN:
                break;
            case SIGNUM:
                break;
            case SINH:
                break;
            case SQRT:
                break;
            case CBRT:
                break;
            case TRUNCATE:
                break;
            case ACOS:
                break;
            case ASIN:
                break;
            case ATAN:
                break;
            case ATAN2:
                break;
            case COS:
                break;
            case COSH:
                break;
            case COT:
                break;
            case DEGREES:
                break;
            case RADIANS:
                break;
            case SIN:
                break;
            case TAN:
                break;
            case ADDDATE:
                break;
            case ADDTIME:
                break;
            case CONVERT_TZ:
                break;
            case DATE:
                break;
            case DATEDIFF:
                break;
            case DATETIME:
                break;
            case DATE_ADD:
                break;
            case DATE_FORMAT:
                break;
            case DATE_SUB:
                break;
            case DAY:
                break;
            case DAYNAME:
                break;
            case DAYOFMONTH:
                break;
            case DAY_OF_MONTH:
                break;
            case DAYOFWEEK:
                break;
            case DAYOFYEAR:
                break;
            case DAY_OF_WEEK:
                break;
            case DAY_OF_YEAR:
                break;
            case EXTRACT:
                break;
            case FROM_DAYS:
                break;
            case FROM_UNIXTIME:
                break;
            case GET_FORMAT:
                break;
            case HOUR:
                break;
            case HOUR_OF_DAY:
                break;
            case LAST_DAY:
                break;
            case MAKEDATE:
                break;
            case MAKETIME:
                break;
            case MICROSECOND:
                break;
            case MINUTE:
                break;
            case MINUTE_OF_DAY:
                break;
            case MINUTE_OF_HOUR:
                break;
            case MONTH:
                break;
            case MONTH_OF_YEAR:
                break;
            case MONTHNAME:
                break;
            case PERIOD_ADD:
                break;
            case PERIOD_DIFF:
                break;
            case QUARTER:
                break;
            case SEC_TO_TIME:
                break;
            case SECOND:
                break;
            case SECOND_OF_MINUTE:
                break;
            case STR_TO_DATE:
                break;
            case SUBDATE:
                break;
            case SUBTIME:
                break;
            case TIME:
                break;
            case TIMEDIFF:
                break;
            case TIME_TO_SEC:
                break;
            case TIMESTAMP:
                break;
            case TIMESTAMPADD:
                break;
            case TIMESTAMPDIFF:
                break;
            case TIME_FORMAT:
                break;
            case TO_DAYS:
                break;
            case TO_SECONDS:
                break;
            case UTC_DATE:
                break;
            case UTC_TIME:
                break;
            case UTC_TIMESTAMP:
                break;
            case UNIX_TIMESTAMP:
                break;
            case WEEK:
                break;
            case WEEKDAY:
                break;
            case WEEKOFYEAR:
                break;
            case WEEK_OF_YEAR:
                break;
            case YEAR:
                break;
            case YEARWEEK:
                break;
            case NOW:
                break;
            case CURDATE:
                break;
            case CURRENT_DATE:
                break;
            case CURTIME:
                break;
            case CURRENT_TIME:
                break;
            case LOCALTIME:
                break;
            case CURRENT_TIMESTAMP:
                break;
            case LOCALTIMESTAMP:
                break;
            case SYSDATE:
                break;
            case TOSTRING:
                break;
            case ADD:
                break;
            case ADDFUNCTION:
                break;
            case DIVIDE:
                break;
            case DIVIDEFUNCTION:
                break;
            case MOD:
                break;
            case MODULUS:
                break;
            case MODULUSFUNCTION:
                break;
            case MULTIPLY:
                break;
            case MULTIPLYFUNCTION:
                break;
            case SUBTRACT:
                break;
            case SUBTRACTFUNCTION:
                break;
            case AND:
                break;
            case OR:
                break;
            case XOR:
                break;
            case NOT:
                break;
            case EQUAL:
//                return new EqualTo()
                break;
            case NOTEQUAL:
                break;
            case LESS:
                break;
            case LTE:
                break;
            case GREATER:
                break;
            case GTE:
                break;
            case LIKE:
                break;
            case NOT_LIKE:
                break;
            case AVG:
                break;
            case SUM:
                break;
            case COUNT:
                break;
            case MIN:
                break;
            case MAX:
                break;
            case VARSAMP:
                break;
            case VARPOP:
                break;
            case STDDEV_SAMP:
                break;
            case STDDEV_POP:
                break;
            case TAKE:
                break;
            case NESTED:
                break;
            case ASCII:
                break;
            case CONCAT:
                break;
            case CONCAT_WS:
                break;
            case LEFT:
                break;
            case LENGTH:
                break;
            case LOCATE:
                break;
            case LOWER:
                break;
            case LTRIM:
                break;
            case POSITION:
                break;
            case REGEXP:
                break;
            case REPLACE:
                break;
            case REVERSE:
                break;
            case RIGHT:
                break;
            case RTRIM:
                break;
            case STRCMP:
                break;
            case SUBSTR:
                break;
            case SUBSTRING:
                break;
            case TRIM:
                break;
            case UPPER:
                break;
            case IS_NULL:
                break;
            case IS_NOT_NULL:
                break;
            case IFNULL:
                break;
            case IF:
                break;
            case NULLIF:
                break;
            case ISNULL:
                break;
            case ROW_NUMBER:
                break;
            case RANK:
                break;
            case DENSE_RANK:
                break;
            case INTERVAL:
                break;
            case CAST_TO_STRING:
                break;
            case CAST_TO_BYTE:
                break;
            case CAST_TO_SHORT:
                break;
            case CAST_TO_INT:
                break;
            case CAST_TO_LONG:
                break;
            case CAST_TO_FLOAT:
                break;
            case CAST_TO_DOUBLE:
                break;
            case CAST_TO_BOOLEAN:
                break;
            case CAST_TO_DATE:
                break;
            case CAST_TO_TIME:
                break;
            case CAST_TO_TIMESTAMP:
                break;
            case CAST_TO_DATETIME:
                break;
            case TYPEOF:
                break;
            case MATCH:
                break;
            case SIMPLE_QUERY_STRING:
                break;
            case MATCH_PHRASE:
                break;
            case MATCHPHRASE:
                break;
            case MATCHPHRASEQUERY:
                break;
            case QUERY_STRING:
                break;
            case MATCH_BOOL_PREFIX:
                break;
            case HIGHLIGHT:
                break;
            case MATCH_PHRASE_PREFIX:
                break;
            case SCORE:
                break;
            case SCOREQUERY:
                break;
            case SCORE_QUERY:
                break;
            case QUERY:
                break;
            case MATCH_QUERY:
                break;
            case MATCHQUERY:
                break;
            case MULTI_MATCH:
                break;
            case MULTIMATCH:
                break;
            case MULTIMATCHQUERY:
                break;
            case WILDCARDQUERY:
                break;
            case WILDCARD_QUERY:
                break;
            default:
                return null;
        }
        return null;
    }
}
