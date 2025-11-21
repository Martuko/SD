-- HadoopPig/app.pig
-- Tarea 3: Análisis batch de vocabulario Yahoo vs LLM
-- Entrada en HDFS: /t3/input/interacciones.csv
-- Formato CSV: source,text
--   source ∈ {'yahoo','llm'}

-- 1) Cargar datos desde HDFS
raw = LOAD '/t3/input/interacciones.csv'
      USING PigStorage(',')
      AS (source:chararray, text:chararray);

-- 2) Eliminar la fila de cabecera (source,text)
no_header = FILTER raw BY (source IS NOT NULL AND source != 'source');

-- 3) Pasar a minúsculas
lowered = FOREACH no_header GENERATE
            source,
            LOWER(text) AS text;

-- 4) Limpiar puntuación:
--    Reemplaza TODO lo que NO sea:
--      - letras (incluyendo acentos y ñ, por si acaso)
--      - dígitos
--      - espacio
--    por un espacio.
cleaned = FOREACH lowered GENERATE
            source,
            REPLACE(
              text,
              '[^A-Za-zÁÉÍÓÚáéíóúÑñ0-9 ]',
              ' '
            ) AS text;

-- 5) Tokenizar por espacios
tokenized = FOREACH cleaned GENERATE
              source,
              FLATTEN(TOKENIZE(text)) AS word;

-- 6) Filtrar tokens vacíos/nulos
nonempty = FILTER tokenized BY
             (word IS NOT NULL) AND
             (word != '');

-- 7) Filtrar stopwords (EN + algunos artefactos tipo "n", "s", "t")
--    Lista básica de stopwords inglesas + pronombres, auxiliares, etc.
filtered = FILTER nonempty BY NOT (
    word MATCHES '^(the|to|you|and|is|i|of|it|in|for|on|that|this|with|as|at|from|by|be|are|was|were|have|has|had|a|an|or|if|but|so|not|your|my|our|their|they|we|he|she|me|him|her|them|there|here|what|which|who|whom|when|where|why|how|can|could|would|should|will|just|about|into|over|than|then|also|too|because|while|during|other|such|do|does|did|done|any|all|every|some|no|yes|up|down|out|more|most|much|many|lot|lots|own|same|very|really|ever|never|maybe|sometimes|often|always|else|again|new|old|still|back|one|two|three|n|s|t|ll|re|ve|d)$'
);

-- 8) Conteo por (source,word)
grp = GROUP filtered BY (source, word);

word_counts = FOREACH grp GENERATE
                FLATTEN(group) AS (source, word),
                COUNT(filtered) AS cnt;

-- 9) Separar Yahoo vs LLM
yahoo = FILTER word_counts BY source == 'yahoo';
llm   = FILTER word_counts BY source == 'llm';

-- 10) Ordenar por frecuencia (desc) y luego alfabéticamente
yahoo_sorted = ORDER yahoo BY cnt DESC, word ASC;
llm_sorted   = ORDER llm   BY cnt DESC, word ASC;

-- 11) Guardar resultados como CSV: source,word,cnt
STORE yahoo_sorted INTO '/t3/output/wordfreq_yahoo'
  USING PigStorage(',');

STORE llm_sorted INTO '/t3/output/wordfreq_llm'
  USING PigStorage(',');
