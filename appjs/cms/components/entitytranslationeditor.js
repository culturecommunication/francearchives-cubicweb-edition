/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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

const {createElement: ce, Component} = require('react'),
    PropTypes = require('prop-types')

const {CustomFieldTemplateConnected} = require('../containers/form'),
    {spinner} = require('./fa'),
    {CmsForm} = require('./editor')

const {
    default: {
        getRelated,
        getSchema,
        getUiSchema,
        relateEntity,
        updateEntity,
        jsonSchemaFetch,
    },
} = require('../api')

const {buildFormData} = require('../utils')
const {AbrevContext} = require('../abrevations')

export class EntityTranslationEditor extends Component {
    constructor(props, context) {
        super(props, context)
        this.language = this.props.match.params.language
        this.state = {
            schema: null,
            uiSchema: null,
            etype: null,
            editMode: false,
            formData: null,
        }
        this.entity = this.props.entity.toJS()
        this.updateSelected = this.updateSelected.bind(this)
        this.onSubmit = this.onSubmit.bind(this)
        this.etype2href = {}
    }

    componentDidMount() {
        // fetch schema for current entity and then all targetSchema in
        // `related.children` links to build data for SelectContentType
        // component
        const {cw_etype, eid} = this.entity
        Promise.all([
            getSchema(cw_etype, eid),
            getRelated(cw_etype, eid, 'translation_of'),
        ]).then(([schema, related]) => {
            const link = schema.links.find(
                    l => l.rel === 'related.translation_of',
                ),
                etype = link.etype
            this.etype2href[link.etype] = link.targetSchema.$ref
            this.updateSelected(etype)
            const selectedTrad = related.find(
                trad => trad.language === this.language,
            )
            if (selectedTrad) {
                this.setState({editMode: true, formData: selectedTrad})
            }
        })
    }

    updateSelected(etype) {
        Promise.all([
            getUiSchema(etype),
            jsonSchemaFetch(this.etype2href[etype]),
        ]).then(([uiSchema, schema]) =>
            this.setState({schema, uiSchema, etype}),
        )
    }

    onSubmit(ev) {
        const {cw_etype, eid} = this.entity,
            {editMode, formData} = this.state
        if (editMode) {
            return updateEntity(
                formData.cw_etype,
                formData.eid,
                ev.formData,
            ).then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.showErrors(doc.errors)
                } else if (doc.absoluteUrl || doc.cwuri) {
                    document.location.replace(doc.absoluteUrl || doc.cwuri)
                }
            })
        } else {
            return relateEntity(
                cw_etype,
                eid,
                'translation_of',
                ev.formData,
                this.state.etype,
            ).then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.showErrors(doc.errors)
                } else if (doc.absoluteUrl || doc.cwuri) {
                    document.location.replace(doc.absoluteUrl || doc.cwuri)
                }
            })
        }
    }

    render() {
        const {etype, schema, uiSchema, editMode, formData} = this.state,
            {errors} = this.props
        const language = this.context.unabrevate(this.language)
        return ce(
            'div',
            {id: 'translation-form'},
            ce(
                'h1',
                null,
                ce('span', null, `${this.entity.i18n_cw_etype} `),
                ce(
                    'a',
                    {href: this.entity.eid, target: '_blank'},
                    `"${this.entity.dc_title}"`,
                ),
            ),
            ce('h2', {}, `Traduction en ${language}`),
            schema
                ? ce(CmsForm, {
                      schema,
                      uiSchema,
                      formData: editMode
                          ? buildFormData(formData, schema)
                          : {language: this.language},
                      onSubmit: this.onSubmit,
                      serverErrors: errors,
                      formContext: {cw_etype: etype},
                      FieldTemplate: CustomFieldTemplateConnected,
                  })
                : ce(spinner),
        )
    }
}

EntityTranslationEditor.propTypes = {
    entity: PropTypes.object.isRequired,
    errors: PropTypes.object.isRequired,
    match: PropTypes.object.isRequired,
    showErrors: PropTypes.func.isRequired,
}

EntityTranslationEditor.contextType = AbrevContext
