# Flatten plugin
Extracts object keys and adds them into the root with some prefix. If provided field isn't object, event will be skipped.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```
Transforms `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

Defines field that should be flattened.

<br>

**`prefix`** *`string`* 

Which prefix to use for extracted fields.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*