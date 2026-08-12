import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import $RefParser from '@apidevtools/json-schema-ref-parser';
import { parse } from 'yaml';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const spec = parse(await readFile(path.join(root, 'openapi.yaml'), 'utf8'));
const dereferenced = await $RefParser.dereference(spec);
const ajv = new Ajv({ strict: false, allowUnionTypes: true });
addFormats(ajv);
const fixtures = [
  ['fixtures/filesystem-collection.json', 'FilesystemCollection'],
  ['fixtures/error-response.json', 'ErrorResponse'],
  ['fixtures/remaining-response.json', 'RemainingResponse'],
];

for (const [fixturePath, schemaName] of fixtures) {
  const schema = dereferenced.components.schemas[schemaName];
  if (!schema) throw new Error(`Missing OpenAPI schema ${schemaName}`);
  const validate = ajv.compile(schema);
  const fixture = JSON.parse(await readFile(path.join(root, fixturePath), 'utf8'));
  if (!validate(fixture)) {
    console.error(validate.errors);
    throw new Error(`${fixturePath} does not conform to ${schemaName}`);
  }
  console.log(`validated ${fixturePath} against ${schemaName}`);
}
