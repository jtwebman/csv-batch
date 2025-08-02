'use strict';

const js = require('@eslint/js');
const prettierPlugin = require('eslint-plugin-prettier');
const globals = require('globals');

module.exports = [
  js.configs.recommended,
  {
    ignores: ['coverage/**', 'node_modules/**', 'package.json']
  },
  {
    languageOptions: {
      ecmaVersion: 2018,
      sourceType: 'script',
      globals: {
        ...globals.node,
        ...globals.mocha
      }
    },
    plugins: {
      prettier: prettierPlugin
    },
    rules: {
      'strict': ['error', 'global'],
      'comma-dangle': 'off',
      'arrow-parens': ['error', 'as-needed'],
      'max-len': ['error', { code: 120 }],
      'indent': ['error', 2],
      'space-before-function-paren': ['error', {
        anonymous: 'always',
        named: 'never',
        asyncArrow: 'ignore'
      }],
      'linebreak-style': 'off'
    }
  }
];