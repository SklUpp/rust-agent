(function_signature
  name: (identifier) @name) @definition.function

(method_signature
  name: (property_identifier) @name) @definition.method

(abstract_method_signature
  name: (property_identifier) @name) @definition.method

(abstract_class_declaration
  name: (type_identifier) @name) @definition.class

(module
  name: (identifier) @name) @definition.module

(interface_declaration
  name: (type_identifier) @name) @definition.interface

(type_annotation
  (type_identifier) @name) @reference.type

(new_expression
  constructor: (identifier) @name) @reference.class

; Added for repo-map coverage: the upstream query omits ordinary declarations.

(function_declaration
  name: (identifier) @name) @definition.function

(generator_function_declaration
  name: (identifier) @name) @definition.function

(class_declaration
  name: (type_identifier) @name) @definition.class

(method_definition
  name: (property_identifier) @name) @definition.method

(lexical_declaration
  (variable_declarator
    name: (identifier) @name
    value: [(arrow_function) (function_expression)])) @definition.function
