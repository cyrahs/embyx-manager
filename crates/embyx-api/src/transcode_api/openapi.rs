//! The OpenAPI operation objects of the /api/transcode routes, as the baseline's document has them
//! (`goldens/api/openapi_transcode.json`) with `$ref`s inlined, the documentation-only keys left out and
//! FastAPI's 422 response added by [`crate::openapi::operation`]. Generated from the
//! baseline's document; the structural test compares the two.

/// Method, path below the router's prefix, and operation object of each route.
const OPERATIONS: &[(&str, &str, &str)] = &[
    (
        "POST",
        "/claim",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "name": {
       "type": "string",
       "maxLength": 128,
       "default": ""
      },
      "version": {
       "type": "string",
       "maxLength": 64,
       "default": ""
      },
      "task_id": {
       "anyOf": [
        {
         "type": "integer"
        },
        {
         "type": "null"
        }
       ]
      },
      "state": {
       "type": "string",
       "maxLength": 32,
       "default": "idle"
      },
      "parts": {
       "items": {
        "properties": {
         "index": {
          "type": "integer"
         },
         "fps": {
          "type": "number",
          "default": 0.0
         },
         "position": {
          "type": "number",
          "default": 0.0
         }
        },
        "type": "object",
        "required": [
         "index"
        ]
       },
       "type": "array",
       "maxItems": 16
      },
      "upload_bps": {
       "type": "number",
       "default": 0.0
      },
      "queue_chunks": {
       "type": "integer",
       "default": 0
      },
      "queue_bytes": {
       "type": "integer",
       "default": 0
      }
     },
     "type": "object"
    }
   }
  },
  "required": true
 },
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "additionalProperties": true,
      "type": "object"
     }
    }
   }
  }
 },
 "security": [
  {
   "HTTPBearer": []
  }
 ]
}"#,
    ),
    (
        "POST",
        "/heartbeat",
        r#"{
 "requestBody": {
  "content": {
   "application/json": {
    "schema": {
     "properties": {
      "name": {
       "type": "string",
       "maxLength": 128,
       "default": ""
      },
      "version": {
       "type": "string",
       "maxLength": 64,
       "default": ""
      },
      "task_id": {
       "anyOf": [
        {
         "type": "integer"
        },
        {
         "type": "null"
        }
       ]
      },
      "state": {
       "type": "string",
       "maxLength": 32,
       "default": "idle"
      },
      "parts": {
       "items": {
        "properties": {
         "index": {
          "type": "integer"
         },
         "fps": {
          "type": "number",
          "default": 0.0
         },
         "position": {
          "type": "number",
          "default": 0.0
         }
        },
        "type": "object",
        "required": [
         "index"
        ]
       },
       "type": "array",
       "maxItems": 16
      },
      "upload_bps": {
       "type": "number",
       "default": 0.0
      },
      "queue_chunks": {
       "type": "integer",
       "default": 0
      },
      "queue_bytes": {
       "type": "integer",
       "default": 0
      }
     },
     "type": "object"
    }
   }
  },
  "required": true
 },
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "additionalProperties": true,
      "type": "object"
     }
    }
   }
  }
 },
 "security": [
  {
   "HTTPBearer": []
  }
 ]
}"#,
    ),
    (
        "GET",
        "/tasks/{task_id}/parts/{part_index}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  },
  {
   "name": "part_index",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {}
    }
   }
  }
 }
}"#,
    ),
    (
        "PUT",
        "/tasks/{task_id}/parts/{part_index}/chunks/{chunk_index}",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  },
  {
   "name": "part_index",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  },
  {
   "name": "chunk_index",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  },
  {
   "name": "x-chunk-start",
   "in": "header",
   "required": true,
   "schema": {
    "type": "number"
   }
  },
  {
   "name": "x-chunk-duration",
   "in": "header",
   "required": true,
   "schema": {
    "type": "number",
    "exclusiveMinimum": 0
   }
  },
  {
   "name": "x-chunk-frames",
   "in": "header",
   "required": true,
   "schema": {
    "type": "integer",
    "exclusiveMinimum": 0
   }
  },
  {
   "name": "x-chunk-sha256",
   "in": "header",
   "required": true,
   "schema": {
    "type": "string",
    "minLength": 64,
    "maxLength": 64
   }
  },
  {
   "name": "content-length",
   "in": "header",
   "required": true,
   "schema": {
    "type": "integer",
    "maximum": 2147483648,
    "exclusiveMinimum": 0
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "type": "object",
      "additionalProperties": true
     }
    }
   }
  }
 }
}"#,
    ),
    (
        "POST",
        "/tasks/{task_id}/parts/{part_index}/finish",
        r#"{
 "security": [
  {
   "HTTPBearer": []
  }
 ],
 "parameters": [
  {
   "name": "task_id",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  },
  {
   "name": "part_index",
   "in": "path",
   "required": true,
   "schema": {
    "type": "integer"
   }
  }
 ],
 "responses": {
  "200": {
   "content": {
    "application/json": {
     "schema": {
      "type": "object",
      "additionalProperties": true
     }
    }
   }
  }
 }
}"#,
    ),
];

/// The operation object of one route, by method and path below the router's prefix.
pub(crate) fn operation(method: &str, path: &str) -> &'static str {
    let Some((_, _, text)) = OPERATIONS.iter().find(|(m, p, _)| *m == method && *p == path) else {
        panic!("no operation {method} {path}");
    };
    text
}
