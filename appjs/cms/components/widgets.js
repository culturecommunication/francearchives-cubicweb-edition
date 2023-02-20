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

const React = require('react')

const {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types')

const DateTimePicker = require('react-widgets/lib/DateTimePicker')
const momentLocalizer = require('react-widgets-moment')

const Moment = require('moment'),
    _ = require('lodash')
Moment.locale('fr')
momentLocalizer()

const {
    default: {getAvailableTargets, getRelated},
} = require('../api')

const {default: Async} = require('react-select/async')

const {spinner} = require('../components/fa')

function formatDate(date, format) {
    const mdate = Moment(date)
    if (mdate && mdate._isValid) {
        return Moment(mdate).format(format)
    }
    return date
}

class DatePicker extends Component {
    constructor(props) {
        super(props)
        this.onChange = this.onChange.bind(this)
        var date = props.value === undefined ? null : new Date(props.value)
        this.state = {value: date}
    }

    onChange(date) {
        this.setState({value: date})
        return date === null
            ? this.props.onChange(undefined)
            : this.props.onChange(formatDate(date, 'YYYY-MM-DD'))
    }

    render() {
        return (
            <DateTimePicker
                time={false}
                format={'DD/MM/YYYY'}
                onChange={this.onChange}
                value={this.state.value}
            />
        )
    }
}

DatePicker.propTypes = {
    value: PropTypes.number,
    onChange: PropTypes.func.isRequired,
}

const Cropper = require('react-cropper').default

function removePropsInDataUrl(value) {
    if (!value) {
        return {value: null, name: null}
    }
    const prefixIdx = value.indexOf(';'),
        prefix = value.slice(0, prefixIdx),
        base64Idx = value.indexOf(';base64'),
        suffix = value.slice(base64Idx)
    let name = null
    for (let prop of value.slice(prefixIdx, base64Idx).split(';')) {
        if (prop.indexOf('=') === -1) {
            continue
        }
        const [propName, propValue] = prop.split('=')
        if (propName === 'name') {
            name = propValue
        }
    }
    return {value: prefix + suffix, name}
}

class ImagePickerComponent extends Component {
    constructor(props) {
        super(props)
        const {value, name} = removePropsInDataUrl(props.value)
        this.state = {
            origSrc: value,
            src: value,
            croppedSrc: null,
            mimetype: null,
            name,
        }
        this.cropper = void 0
        this.onChange = this.onChange.bind(this)
        this.onUpload = this.onUpload.bind(this)
        this.cropImage = this.cropImage.bind(this)
        this.resetCrop = this.resetCrop.bind(this)
    }

    onUpload(e) {
        var _this = this
        e.preventDefault()
        var files = void 0
        if (e.dataTransfer) {
            files = e.dataTransfer.files
        } else if (e.target) {
            files = e.target.files
        }
        _this.setState({name: files[0].name})
        var reader = new FileReader()
        reader.onload = function () {
            var url = reader.result
            _this.setState({src: url, origSrc: url})
            _this.onChange(url, files[0].name)
        }
        reader.readAsDataURL(files[0])
    }

    onChange(url, filename) {
        const base64Index = url.indexOf(';base64')
        url =
            url.slice(0, base64Index) +
            `;name=${filename}` +
            url.slice(base64Index)
        this.props.onChange(url)
    }

    cropImage() {
        if (typeof this.cropper.getCroppedCanvas() === 'undefined') {
            return null
        }
        var url = this.cropper.getCroppedCanvas().toDataURL()
        this.setState({croppedSrc: url, src: url})
        return this.onChange(url, this.state.name)
    }

    resetCrop() {
        this.setState({src: this.state.origSrc, croppedSrc: null})
        return this.onChange(this.state.src, this.state.name)
    }

    render() {
        return ce(
            'div',
            null,
            ce('input', {
                type: 'file',
                src: this.props.src,
                onChange: this.onUpload,
            }),
            ce(Cropper, {
                ref: (cropper) => {
                    this.cropper = cropper
                },
                src: this.state.src,
                style: {width: '100%'},
                aspectRatio: this.props.aspectRatio,
                preview: '.image-preview',
            }),
            ce(
                'div',
                {style: {paddingTop: '1em', paddingBottom: '1em'}},
                ce(
                    'a',
                    {className: 'btn btn-default', onClick: this.cropImage},
                    'crop',
                ),
                ce(
                    'a',
                    {className: 'btn btn-default', onClick: this.resetCrop},
                    'reset crop',
                ),
            ),
            ce(
                'div',
                null,
                ce('img', {
                    style: {width: '100%'},
                    src: this.state.croppedSrc,
                }),
            ),
        )
    }
}

ImagePickerComponent.propTypes = {
    value: PropTypes.string,
    src: PropTypes.string,
    aspectRatio: PropTypes.float,
    onChange: PropTypes.func.isRequired,
}

class ImagePicker extends Component {
    render() {
        return ce(ImagePickerComponent, {
            ...this.props,
            aspectRatio: 16 / 9,
        })
    }
}

ImagePicker.propTypes = {
    value: PropTypes.string,
    src: PropTypes.string,
    onChange: PropTypes.func.isRequired,
}

class SubjectImagePicker extends Component {
    render() {
        return ce(ImagePickerComponent, {
            ...this.props,
            aspectRatio: 2 / 1,
        })
    }
}

SubjectImagePicker.propTypes = {
    value: PropTypes.string,
    src: PropTypes.string,
    onChange: PropTypes.func.isRequired,
}

class AutoCompleteField extends Component {
    constructor(props) {
        super(props)
        this.state = {
            rtype: props.name,
            formData: props.formData,
            required: props.required,
            related: [],
            multi: true, //FIXME
        }
        this.onChange = this.onChange.bind(this)
    }

    onChange(value) {
        const ids = value.map((e) => String(e.value))
        this.setState({related: value, formData: ids})
        this.props.onChange(ids)
    }

    setEmptyFormData() {
        this.setState({formData: []})
    }

    componentDidMount() {
        const {rtype} = this.state,
            {cw_etype, eid} = this.props.formContext
        if (eid !== undefined) {
            Promise.all([getRelated(cw_etype, eid, rtype)]).then(
                ([related]) => {
                    if (this.state.formData === undefined) {
                        // lack of oneOf support
                        const ids = related.map((r) => String(r.eid))
                        this.setState({formData: ids})
                    }
                    const targets = related.map((r) => ({
                        value: r.eid,
                        label: r.dc_title,
                    }))
                    this.setState({related: targets})
                },
            )
        } else {
            if (this.state.formData === undefined) {
                this.setEmptyFormData()
            }
        }
    }

    render() {
        const {rtype, required, related, multi} = this.state,
            {cw_etype, eid} = this.props.formContext
        if (related === null) {
            return ce(spinner)
        }

        function loadOptions(input) {
            if (input.length < 3) {
                return []
            }
            return getAvailableTargets(cw_etype, rtype, eid, input).then((d) =>
                d.map((e) => ({label: e.title, value: e.eid})),
            )
        }
        const labelClass = required ? 'control-label required' : 'control-label'
        return ce(
            'div',
            {className: 'mb-4'},
            ce(
                'label',
                {className: labelClass},
                this.props.schema.title || rtype,
            ),
            ce(Async, {
                isMulti: multi,
                name: rtype,
                loadOptions: _.throttle(loadOptions, 300),
                onBlurResetsInput: false,
                placeholder: 'Rechercher',
                value: related,
                isClearable: true,
                noOptionsMessage: () => 'Aucune entité trouvée',
                onChange: this.onChange,
            }),
        )
    }
}
AutoCompleteField.propTypes = {
    formContext: PropTypes.shape({
        cw_etype: PropTypes.string,
        eid: PropTypes.number,
    }),
    name: PropTypes.string,
    required: PropTypes.bool,
    formData: PropTypes.object,
    schema: PropTypes.object,
    onChange: PropTypes.func.isRequired,
}

class FilePicker extends Component {
    constructor(props) {
        super(props)
        this.onChange = this.onChange.bind(this)
    }

    onChange(ev) {
        const file = ev.target.files[0]
        this.props.onChange(
            `${_.last(file.type.split('/'))}/${file.name}-${file.size}`,
        )
    }

    render() {
        return ce(
            'div',
            null,
            'filepicker :',
            ce('input', {type: 'file', onChange: this.onChange}),
        )
    }
}

FilePicker.propTypes = {
    onChange: PropTypes.func.isRequired,
}

module.exports = {
    FilePicker,
    DatePicker,
    ImagePicker,
    SubjectImagePicker,
    AutoCompleteField,
}
