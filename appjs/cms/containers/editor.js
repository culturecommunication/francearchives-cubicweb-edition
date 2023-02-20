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
const {createElement: ce, Component} = require('react'),
    PropTypes = require('prop-types'),
    {connect} = require('react-redux')

const {showErrors} = require('../actions'),
    {CmsForm} = require('../components/editor'),
    {spinner} = require('../components/fa'),
    {CustomFieldTemplateConnected} = require('./form')

const {
    default: {updateEntity, getSchema, getUiSchema, getEntity},
} = require('../api')
const {buildFormData} = require('../utils')

class BaseContentEditor extends Component {
    constructor(props) {
        super(props)
        this.customValidate = this.customValidate.bind(this)
        this.onSubmit = this.onSubmit.bind(this)
        this.state = {
            schema: null,
            uischema: null,
            formData: null,
        }
    }

    componentDidMount() {
        const {eid, cw_etype} = this.props.entity
        Promise.all([
            getSchema(cw_etype, eid, 'edition'),
            getUiSchema(cw_etype),
            getEntity(cw_etype, eid),
        ]).then(([schema, uiSchema, formData]) =>
            this.setState({schema, uiSchema, formData}),
        )
    }

    customValidate(_, errors) {
        return errors
    }

    onSubmit(ev) {
        const {cw_etype, eid} = this.props.entity
        this.setState({formData: ev.formData})
        return updateEntity(cw_etype, eid, ev.formData).then((doc) => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors))
            } else {
                document.location.reload()
            }
        })
    }

    render() {
        const {schema, uiSchema, formData, errors} = this.state
        const {eid, cw_etype} = this.props.entity
        if (schema === null) {
            return ce(spinner)
        }

        return ce(CmsForm, {
            schema: schema,
            formData: buildFormData(formData, schema),
            formContext: {eid: eid, cw_etype: cw_etype},
            uiSchema: uiSchema,
            validate: this.customValidate,
            onSubmit: this.onSubmit,
            serverErrors: errors,
            FieldTemplate: CustomFieldTemplateConnected,
        })
    }
}
BaseContentEditor.propTypes = {
    entity: PropTypes.shape({
        cw_etype: PropTypes.string.isRequired,
        eid: PropTypes.number.isRequired,
    }).isRequired,
    dispatch: PropTypes.func.isRequired,
}

class NewsContentEditor extends BaseContentEditor {
    customValidate(formData, errors) {
        errors = super.customValidate(formData, errors)
        if (formData.stop_date !== undefined && formData.stop_date.length) {
            const start = new Date(formData.start_date),
                stop = new Date(formData.stop_date)
            if (start > stop) {
                errors.stop_date.addError(
                    'La date de fin doit être supérieure ou égale à la date de début',
                )
                errors.start_date.addError(
                    'La date de debut doit être inférieure ou égale à la date de début',
                )
            }
        }
        return errors
    }
}

const MAPS = {
    NewsContent: NewsContentEditor,
}

class Editor extends Component {
    constructor(props, context) {
        super(props, context)
        this.entity = this.props.entity.toJS
            ? this.props.entity.toJS()
            : this.props.entity
        this.errors = this.props.errors.toJS()
    }

    render() {
        const {dispatch} = this.props,
            {entity, errors} = this
        const comp = MAPS[entity.cw_etype] || BaseContentEditor
        return ce(comp, {entity, errors, dispatch})
    }
}
Editor.propTypes = {
    entity: PropTypes.object.isRequired,
    errors: PropTypes.object.isRequired,
    dispatch: PropTypes.func.isRequired,
}

module.exports = connect(function mapStateToProps(state, props) {
    const entity = props.entity || state.getIn(['model', 'entity']),
        errors = state.getIn(['app', 'errors'])
    return {entity, errors}
})(Editor)
