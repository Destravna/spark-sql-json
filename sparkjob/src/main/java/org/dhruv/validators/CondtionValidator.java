package org.dhruv.validators;

import java.util.Arrays;

import org.dhruv.exceptions.InvalidFieldValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondtionValidator {

    private static final Logger log = LoggerFactory.getLogger(CondtionValidator.class);

    private static String[] stringTransformations = { "upper", "lower" };
    private static String[] longTranformations = { "round", "abs" };

    public static void validateTransformation(String transformation, String type) throws InvalidFieldValue {
        switch (type) {
            case "string":
                if (Arrays.asList(stringTransformations).contains(transformation)) {
                    return;
                } else {
                    logUnrecognizedTransformation(transformation);
                    throw new InvalidFieldValue(transformation + ": not a recognised transformation");
                }
            case "long":
                if (Arrays.asList(longTranformations).contains(transformation)) {
                    return;
                } else {
                    logUnrecognizedTransformation(transformation);
                    throw new InvalidFieldValue(transformation + ": not a recognised transformation");
                }
            default :
                throw new InvalidFieldValue("invalid conditions please remove");
        }

    }

    private static void logUnrecognizedTransformation(String transformation) {

    }

}
