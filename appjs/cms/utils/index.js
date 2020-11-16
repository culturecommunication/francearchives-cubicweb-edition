/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
import {forOwn, get, has} from 'lodash/object'

export function buildFormData(entity, schema) {
    // Return a formData object suitable for usage in a
    // react-jsonschema-form's Form with `schema`. This is built by filter out
    // `entity` JSON document from fields that do no not appear in
    // `schema`.
    let eschema = null
    if (schema.definitions !== undefined) {
        eschema = schema.definitions[entity.cw_etype] || null
    }
    if (eschema === null) {
        eschema = schema
    }
    const formData = {}

    function iteratee(value, key) {
        const target = get(entity, key, null)
        if (target === null) {
            return
        }
        if (value.type === 'array') {
            if (!has(value, 'items.$ref')) {
                console.warn(
                    'unhandled items kind',
                    value,
                    `in ${key} property`,
                )
                return
            }
            const ref = get(value, 'items.$ref')
            const prefix = '#/definitions/'
            if (!ref.startsWith(prefix)) {
                throw new Error(`unhandled reference kind ${ref}`)
            }
            const targetType = ref.slice(prefix.length)
            if (!has(schema, 'definitions', targetType)) {
                throw new Error(
                    `missing definition for ${targetType} referenced in ${key}`,
                )
            }
            formData[key] = target.map(tgt => buildFormData(tgt, schema))
        } else {
            formData[key] = target
        }
    }
    if (eschema === undefined) {
        return entity
    }
    forOwn(eschema.properties, iteratee)
    return formData
}
