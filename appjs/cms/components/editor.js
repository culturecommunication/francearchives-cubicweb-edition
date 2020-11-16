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
const {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types')

const {default: Form} = require('react-jsonschema-form')

const {TinyMCEWrapper} = require('./ckeditor'),
    {spinner} = require('./fa'),
    {
        FilePicker,
        DatePicker,
        ImagePicker,
        AutoCompleteField,
    } = require('./widgets'),
    {showErrors} = require('../actions'),
    {CustomFieldTemplateConnected} = require('../containers/form')

const {
    default: {getSchema, getUiSchema, createEntity},
} = require('../api')

const {buildFormData} = require('../utils')

function ErrorListTop({errors}) {
    if (!errors || errors.length === 0) {
        return null
    }
    return ce(
        'div',
        {className: 'panel panel-danger errors'},
        ce(
            'div',
            {className: 'panel-heading'},
            ce('h3', {className: 'panel-title'}, 'Errors'),
        ),
        ce(
            'ul',
            {className: 'list-group'},
            errors.map((error, i) =>
                ce(
                    'li',
                    {key: i, className: 'list-group-item text-danger'},
                    error.stack,
                ),
            ),
        ),
    )
}

class CmsForm extends Form {
    renderErrors() {
        const {status, errors} = this.state
        const {showErrorList, serverErrors} = this.props
        if (serverErrors) {
            serverErrors.forEach(err => {
                if (err.source && err.source.pointer) {
                    errors.push({
                        stack: `${err.source.pointer}: ${err.details}`,
                    })
                } else {
                    errors.unshift({stack: `${err.details}`})
                }
            })
        }
        if (status !== 'editing' && errors.length && showErrorList !== false) {
            return ce(ErrorListTop, {errors})
        }
        return null
    }
}

CmsForm.defaultProps = {
    widgets: {
        wysiwygEditor: TinyMCEWrapper,
        dateEditor: DatePicker,
        imageEditor: ImagePicker,
        autocompleteField: AutoCompleteField,
        filepicker: FilePicker,
    },
    fields: {
        autocompleteField: AutoCompleteField,
    },
}

const location_reload = document.location.reload.bind(document.location)

class CmsFormWrapper extends Component {
    constructor(props, context) {
        super(props, context)
        this.onSubmit = this.onSubmit.bind(this)
        this.onCancel = this.props.onCancel || location_reload
        this.state = {loading: false, formData: null, errors: null}
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        this.onCancel = nextProps.onCancel || location_reload
    }

    componentWillUnmount() {
        this.unmounted = true
    }

    onSubmit(ev) {
        this.setState({formData: ev.formData, loading: true})
        return this.props.onSubmit(ev).then(() => {
            if (!this.unmounted) {
                this.setState({loading: false})
            }
        })
    }

    render() {
        const {loading, formData} = this.state,
            formProps = Object.assign({formData: formData}, this.props)
        if (this.props.children !== undefined) {
            return ce(CmsForm, formProps)
        }
        formProps.onSubmit = this.onSubmit
        return ce(
            CmsForm,
            formProps,
            ce(
                'div',
                {className: 'btn-group'},
                ce(
                    'button',
                    {
                        type: 'button',
                        onClick: this.onCancel,
                        className: 'btn btn-default',
                    },
                    'annuler',
                ),
                ce(
                    'button',
                    {type: 'submit', className: 'btn btn-primary'},
                    loading ? ce(spinner) : '',
                    'enregistrer',
                ),
            ),
        )
    }
}

CmsFormWrapper.propTypes = {
    onCancel: PropTypes.func,
    onSubmit: PropTypes.func.isRequired,
    children: PropTypes.node,
}

exports.CmsForm = CmsFormWrapper

class AddEntityForm extends Component {
    constructor(props) {
        super(props)
        this.state = {
            schema: null,
            uiSchema: null,
            formData: props.formData || null,
            errors: null,
        }
        this.onSubmit = this.onSubmit.bind(this)
        this.formTitle = this.props.formTitle || null
        this.customValidate = this.customValidate.bind(this)
        this.onChange = this.onChange.bind(this)
    }

    onChange(formState) {
        this.setState({
            formData: buildFormData(formState.formData, this.state.schema),
        })
        if (this.props.onChange) {
            this.props.onChange(formState)
        }
    }

    customValidate(_, errors) {
        return errors
    }

    onSubmit(ev) {
        this.setState({formData: ev.formData})
        return createEntity(this.props.etype, ev.formData).then(doc => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors))
                return
            } else if (doc.absoluteUrl) {
                document.location.replace(doc.absoluteUrl)
            }
        })
    }

    componentDidMount() {
        Promise.all([
            getSchema(this.props.etype, null, 'creation'),
            getUiSchema(this.props.etype),
        ]).then(([schema, uiSchema]) => this.setState({schema, uiSchema}))
    }

    render() {
        const {schema, uiSchema, formData, errors} = this.state
        let body
        if (schema === null) {
            body = ce(spinner)
        } else {
            body = ce(CmsFormWrapper, {
                schema,
                uiSchema,
                formData,
                onChange: this.props.onChange ? this.onChange : undefined,
                serverErrors: errors,
                FieldTemplate: CustomFieldTemplateConnected,
                validate: this.customValidate,
                onSubmit: this.onSubmit,
            })
        }
        let title =
            this.formTitle === null ? null : ce('h1', null, this.formTitle)
        return ce('div', null, title, body)
    }
}

AddEntityForm.propTypes = {
    onChange: PropTypes.func,
    formData: PropTypes.object,
    dispatch: PropTypes.func.isRequired,
    etype: PropTypes.string.isRequired,
    formTitle: PropTypes.string,
}

exports.AddEntityForm = AddEntityForm
