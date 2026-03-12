# Cryptographic functions

The following cryptographic functions are supported in PPL.

## MD5

**Usage**: `MD5(str)`

Calculates the MD5 digest and returns the value as a 32-character hex string.

**Parameters**:

- `str` (Required): The string for which to calculate the MD5 digest.

**Return type**: `STRING`

#### Example
  
```ppl
source=people
| eval `MD5('hello')` = MD5('hello')
| fields `MD5('hello')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------+
| MD5('hello')                     |
|----------------------------------|
| 5d41402abc4b2a76b9719d911017c592 |
+----------------------------------+
```
  
## SHA1

**Usage**: `SHA1(str)`

Returns the SHA-1 hash as a hex string.

**Parameters**:

- `str` (Required): The string for which to calculate the SHA-1 hash.

**Return type**: `STRING`

#### Example
  
```ppl
source=people
| eval `SHA1('hello')` = SHA1('hello')
| fields `SHA1('hello')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------------------------+
| SHA1('hello')                            |
|------------------------------------------|
| aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d |
+------------------------------------------+
```
  
## SHA2

**Usage**: `SHA2(str, numBits)`

Returns the result of SHA-2 family hash functions (SHA-224, SHA-256, SHA-384, and SHA-512) as a hex string.

**Parameters**:

- `str` (Required): The string for which to calculate the SHA-2 hash.
- `numBits` (Required): The desired bit length of the result, which must be `224`, `256`, `384`, or `512`.

**Return type**: `STRING`

#### Example: SHA-256 hash
  
```ppl
source=people
| eval `SHA2('hello',256)` = SHA2('hello',256)
| fields `SHA2('hello',256)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------------------------------------------------+
| SHA2('hello',256)                                                |
|------------------------------------------------------------------|
| 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 |
+------------------------------------------------------------------+
```

#### Example: SHA-512 hash
  
```ppl
source=people
| eval `SHA2('hello',512)` = SHA2('hello',512)
| fields `SHA2('hello',512)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------------------------------------------------------------+
| SHA2('hello',512)                                                                                                                |
|----------------------------------------------------------------------------------------------------------------------------------|
| 9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043 |
+----------------------------------------------------------------------------------------------------------------------------------+
```
  
