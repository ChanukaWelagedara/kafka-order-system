package com.bigdata.kafkaordersystem.exception;
import com.bigdata.kafkaordersystem.dto.ApiResponse;
import com.fasterxml.jackson.core.JsonParseException;
import jakarta.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ApiResponse> handleNotFound(NotFoundException ex) {
        log.warn(ex.getMessage());
        return ResponseEntity.status(404).body(new ApiResponse("404", ex.getMessage(), null, false));
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ApiResponse> handleValidation(ValidationException ex) {
        log.warn(ex.getMessage());
        return ResponseEntity.badRequest().body(new ApiResponse("400", ex.getMessage(), null, false));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse> handleMethodArgumentNotValid(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getAllErrors().forEach((error) -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            errors.put(fieldName, errorMessage);
        });

        log.warn("Validation failed: {}", errors);
        return ResponseEntity.badRequest().body(
            new ApiResponse("400", "Validation failed", errors, false)
        );
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse> handleHttpMessageNotReadable(HttpMessageNotReadableException ex) {
        String errorMessage = "Invalid JSON format";

        if (ex.getCause() instanceof JsonParseException) {
            JsonParseException jsonEx = (JsonParseException) ex.getCause();
            errorMessage = "JSON parse error: " + jsonEx.getOriginalMessage() +
                          ". Please check your JSON syntax (missing commas, quotes, or invalid values)";
        } else if (ex.getMessage().contains("JSON parse error")) {
            errorMessage = "Invalid JSON: Please ensure proper formatting with commas between fields, " +
                          "correct data types (price must be a number), and no extra keywords like 'double'";
        }

        log.error("JSON parsing error: {}", errorMessage);
        return ResponseEntity.badRequest().body(
            new ApiResponse("400", errorMessage, null, false)
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse> handleGeneral(Exception ex) {
        log.error(ex.getMessage(), ex);
        return ResponseEntity.status(500).body(new ApiResponse("500", "Internal server error", null, false));
    }
}